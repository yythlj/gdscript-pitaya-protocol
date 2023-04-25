extends Node

const host = "localhost"
const port = 3850


const packet_Handshake = 0x01
const packet_HandshakeAck = 0x02
const packet_Heartbeat = 0x03
const packet_Data = 0x04
const packet_Kick = 0x05
const HeadLength    = 4
const MaxPacketSize = 1 << 24 #16MB

const message_Request = 0x00
const message_Notify = 0x01
const message_Response = 0x02
const message_Push = 0x03



const message_errorMask = 0x20
const message_gzipMask = 0x10
const message_msgRouteCompressMask = 0x01
const message_msgTypeMask = 0x07
const message_msgRouteLengthMask = 0xFF
const message_msgHeadLength = 0x02



var wait_cb = {}
var lastMsgId = 0
var client = null
var clientHandshakeData = null
var packet_encoder = null
var packet_decoder = null
var message_encoder = null
var heartbeat_thread = null
var handle_thread = null
var repear_thread = null
var compression = null
var heartbeatTimeout = -1
var requestTimeout = 5

var Connected = false


func _ready():
	print("connect ready")
	compression = Compression.new()
	packet_encoder = PomeloPacketEncoder.new()
	packet_decoder = PomeloPacketDecoder.new()
	message_encoder = MessagesEncoder.new()
	message_encoder.compression = compression
	client = StreamPeerTCP.new()
	connectServer()
	set_process(false)

func newMsgID():
	lastMsgId += 1
	return lastMsgId

#SendRequest sends a request to the server
func SendRequest(route, data, cb):
	var m = Message.new()
	m.Type = message_Request
	m.ID = newMsgID()
	m.Route = route
	m.Data = data
	m.SentAt = OS.get_unix_time()
	m.Callback = cb
	var p = buildPacket(m)
	#c.pendingReqMutex.Lock()
	wait_cb[m.ID] = m
	#c.pendingReqMutex.Unlock()
	client.put_data(p)

#SendNotify sends a notify to the server
func SendNotify(route , data):
	var m = Message.new()
	m.Type = message_Notify
	m.ID = newMsgID()
	m.Route = route
	m.Data = data
	m.SentAt = OS.get_unix_time()
	var p = buildPacket(m)
	client.put_data(p)

func buildPacket(msg):
	var encMsg = message_encoder.Encode(msg)
	var p = packet_encoder.Encode(packet_Data, encMsg)
	return p

func readPackets():
	#listen for sv messages
	var data = PoolByteArray()
	var n = 0
	var dataA = PoolByteArray()
	while n == len(dataA):
		n = min(client.get_available_bytes(), 1024)
		if n > 0:
			var _readinfo = client.get_data(n)
			if _readinfo[0] == OK:
				dataA = _readinfo[1]
				data.append_array(dataA)
			else:
				n = 0
			
	var packets = packet_decoder.Decode(data)
	if packets == null:
		print("net read fail" + str(len(data)) +"   "+ str(len(dataA)) + " " + str(n))
		return null
	return packets

func connectServer():
	print("net connect...")
	var err = client.connect_to_host(host, port)
	if err != OK:
		print("net connect fail")
		return
	client.set_no_delay(true)
	var p = packet_encoder.Encode(packet_Handshake, PoolByteArray())
	if p == null:
		print("net connect fail")
		return
	client.put_data(p)
	var packets = readPackets()
	if packets == null:
		print("net connect ack fail")
		return err
	var handshakePacket = packets[0]
	
	if handshakePacket.Type != packet_Handshake:
		return
	print("get handshake " + str(handshakePacket.Data))
	if compression.IsCompressed(handshakePacket.Data):
		handshakePacket.Data = compression.InflateData(handshakePacket.Data)
		if handshakePacket.Data == null:
			print("comp err")
			return
	var buf = handshakePacket.Data

	var ret_code = buf[0]
	buf = buf.subarray(1, -1)
	
	heartbeatTimeout = buf[0]
	buf = buf.subarray(1, -1)
	
	var _strlen = buf[0]
	var serializerName = buf.subarray(1, _strlen).get_string_from_utf8()
	buf = buf.subarray(_strlen + 1, -1)
	
	var _routelen = buf[0]
	buf = buf.subarray(1, -1) if len(buf) > 1 else []
	var routeDict = {}
	for i in range(_routelen):
		var _routestrlen = buf[0]
		var _route = buf.subarray(1, _routestrlen).get_string_from_utf8()
		buf = buf.subarray(_routestrlen + 1, -1)
		var _code = buf[0] << 8 | buf[1]
		routeDict[_route] = _code
		buf = buf.subarray(2, -1) if len(buf) > 2 else []
		
	message_encoder.SetDictionary(routeDict)
	print("handshake   "+ str(routeDict) + str(serializerName))
	p = packet_encoder.Encode(packet_HandshakeAck, PoolByteArray())
	if p == null:
		return

	client.put_data(p)

	Connected = true
	heartbeat_thread = Thread.new()
	heartbeat_thread.start(self, "sendHeartbeats", heartbeatTimeout)

	handle_thread = Thread.new()
	handle_thread.start(self, "handleServerMessage")
	
	repear_thread = Thread.new()
	repear_thread.start(self, "requestReaper")
	return null
	
func sendHeartbeats(delta):
	while true:
		var p = packet_encoder.Encode(packet_Heartbeat, PoolByteArray())
		client.put_data(p)
		yield(get_tree().create_timer(delta), "timeout")

func handleServerMessage():
	#defer c.Disconnect()
	while Connected:
		var packets = readPackets()
		if packets == null && Connected:
			break
		for p in packets:
			handlePackets(p)

func handlePackets(p):
	if p.Type == packet_Data:
		#handle data
		var m = message_encoder.Decode(p.Data)
		OnSrvPacket(m)
	elif p.Type == packet_Kick:
		client.disconnect_from_host()

func requestReaper():
	while Connected:
		#c.pendingReqMutex.Lock()
		for msgid in wait_cb.keys():
			var msg = wait_cb[msgid]
			if OS.get_unix_time() - msg.SendAt > requestTimeout:
				wait_cb.erase(msgid)
				OnTimeoutMsg(msg)
		#c.pendingReqMutex.Unlock()
		yield(get_tree().create_timer(1), "timeout")

func OnSrvPacket(m):
	if m.Type == message_Response:
		#c.pendingReqMutex.Lock()
		if m.ID in wait_cb:
			var cb_msg = wait_cb[m.ID]
			wait_cb.erase(m.ID)
			cb_msg.Callback(true, cb_msg, m)
		#c.pendingReqMutex.Unlock()
	else:
		OnPushMsg(m)
		
func OnPushMsg(msg):
	pass

func OnTimeoutMsg(msg):
	msg.Callback(false, msg, null)

	
func _exit_tree():
	Connected = false
	heartbeat_thread.wait_to_finish()
	handle_thread.wait_to_finish()
	repear_thread.wait_to_finish()

class Packet:
	var Type = -1
	var Length = -1
	var Data = PoolByteArray()

class PomeloPacketEncoder:
#Encode create a packet.Packet from  the raw bytes slice and then encode to network bytes slice
#Protocol refs: https://github.com/NetEase/pomelo/wiki/Communication-Protocol
#
# -<type>-|--------<length>--------|-<data>-
# --------|------------------------|--------
# 1 byte packet type, 3 bytes packet data length(big end), and data segment
	func Encode(typ, data):
		if typ < packet_Handshake || typ > packet_Kick:
			return null
		if len(data) > MaxPacketSize:
			return null
		var buf = PoolByteArray()
		buf.append(typ)
		buf.append_array(IntToBytes(len(data)))
		buf.append_array(data)
		return buf

	func IntToBytes(n):
		var buf = PoolByteArray()
		buf.append((n >> 16) & 0xFF)
		buf.append((n >> 8) & 0xFF)
		buf.append(n & 0xFF)
		return buf

class PomeloPacketDecoder:

	func BytesToInt(b):
		var result = 0
		for bytev in b:
			result = int(result<<8) + int(bytev)
		return result

	func ParseHeader(header):
		if len(header) != HeadLength:
			return null
		var typ = header[0]
		if typ < packet_Handshake || typ > packet_Kick:
			print("err header nu" + str(typ))
			return null
		var size = BytesToInt(header.subarray(1, -1))
		if size > MaxPacketSize:
			print("err size" + str(size))
			return null
		return [size, typ]
	
	#Decode decode the network bytes slice to packet.Packet(s)
	func Decode(buf):
		# check length
		if len(buf) < HeadLength:
			print("err header 0 ")
			return null

		# first time
		var header = buf.subarray(0, HeadLength - 1)
		buf = buf.subarray(HeadLength, -1) if len(buf) > HeadLength else []
		var info = ParseHeader(header)
		if info == null:
			print("err header 1 " + str(len(header)))
			return null
		var size = info[0]
		var typ = info[1] 
		var packets = []
		while size <= len(buf):
			var p = Packet.new()
			p.Type = typ
			p.Length = size
			packets.append(p)
			if size > 0:
				p.Data = buf.subarray(0, size - 1)
				buf = buf.subarray(size, -1) if size < len(buf) else []
			if len(buf) < HeadLength:
				break
			header = buf.subarray(0, HeadLength-1)
			buf = buf.subarray(HeadLength, -1) if HeadLength < len(buf) else []
			info = ParseHeader(header)
			if info == null:
				break
			size = info[0]
			typ = info[1]
		return packets

class MessagesEncoder:
	var DataCompression = false
	var compression = null
	var routes = {}#压缩字典
	var codes = {}
	
	func SetDictionary(dict):
		if dict == null:
			return null
		#routesCodesMutex.Lock()
		#defer routesCodesMutex.Unlock()

		for route in dict:
			var code = dict[route]
			var r = route.replace(" ", "")

			#duplication check
			if r in routes:
				return -1
			if code in codes:
				return -2
			#update map, using last value when key duplicated
			routes[r] = code
			codes[code] = r

		return null

	#GetDictionary gets the routes map which is used to compress route.
	func GetDictionary():
		#routesCodesMutex.RLock()
		#defer routesCodesMutex.RUnlock()
		var dict = {}
		for k in routes:
			dict[k] = routes[k]
		return dict
	
	func routable(t):
		return t == message_Request || t == message_Notify || t == message_Push
	
	func invalidType(t):
		return t < message_Request || t > message_Push
		
	func Uint16(b):
		var b1 = b[1] & 0xFFFF
		var b2 = (b[0] & 0xFFFF)<<8
		return b1 | b2
	
	func Encode(message):
		if invalidType(message.Type):
			return null
		var buf = PoolByteArray()
		var flag = (message.Type & 0xFF) << 1

		#routesCodesMutex.RLock()
		var info = routes[message.Route]
		var code = info[0]
		var compressed = info[1]
		#routesCodesMutex.RUnlock()
		if compressed:
			flag |= message_msgRouteCompressMask

		if message.Err:
			flag |= message_errorMask

		buf.append(flag)

		if message.Type == message_Request || message.Type == message_Response:
			var n = message.ID
			#variant length encode
			while true:
				var b = (n % 128) & 0xFF
				n = n/128
				if n != 0:
					buf.append(b+128)
				else:
					buf.append(b)
					break

		if routable(message.Type):
			if compressed:
				buf.append((code>>8)&0xFF)
				buf.append(code&0xFF)
			else:
				buf.append(len(message.Route)&0xFF)
				buf.append_array(message.Route.to_utf8())


		if DataCompression:
			var d = compression.DeflateData(message.Data)
			if d == null:
				return null

			if len(d) < len(message.Data):
				message.Data = d
				buf[0] |= message_gzipMask

		buf.append_array(message.Data)
		return buf


	# Decode unmarshal the bytes slice to a message
	# See ref: https://github.com/topfreegames/pitaya/v2/blob/master/docs/communication_protocol.md
	func Decode(data):
		if len(data) < message_msgHeadLength:
			return null
		var m = Message.new()
		var flag = data[0]
		var offset = 1
		m.Type = (0xFF & (flag >> 1)) & message_msgTypeMask

		if invalidType(m.Type):
			return null

		if m.Type == message_Request || m.Type == message_Response:
			var id = 0
			# little end byte order
			# WARNING: must can be stored in 64 bits integer
			# variant length encode
			var i = offset
			while i < len(data):
				var b = data[i]
				id += (b&0x7F) * int(pow(2, (7 *i)))
				if b < 128:
					offset = i + 1
					break
				i += 1	
			m.ID = id

		m.Err = flag&message_errorMask == message_errorMask

		if routable(m.Type):
			if flag&message_msgRouteCompressMask == 1:
				m.compressed = true
				var code = Uint16(data.subarray(offset, offset + 1))
				#routesCodesMutex.RLock()
				var route = codes.get(code, null)
				#routesCodesMutex.RUnlock()
				if route == null:
					return null
				
				m.Route = route
				offset += 2
			else:
				m.compressed = false
				var rl = data[offset]
				offset += 1
				var valb = data.subarray(offset, offset + int(rl) - 1)
				m.Route = valb.get_string_from_utf8()
				offset += int(rl)


		m.Data = data.subarray(offset, -1)
		if flag&message_gzipMask == message_gzipMask:
			m.Data = compression.InflateData(m.Data)
			if m.Data == null:
				return null
		
		return m

class Compression:
	func DeflateData(data):
		var bb = PoolByteArray()
		#未实现
		return bb

	func InflateData(data):
		var bb = PoolByteArray()
		#未实现
		return bb

	func IsCompressed(data):
		return len(data) > 2 &&(
			# zlib
			(data[0] == 0x78 &&
			(data[1] == 0x9C ||
			data[1] == 0x01 ||
			data[1] == 0xDA ||
			data[1] == 0x5E)) ||
			#gzip
			(data[0] == 0x1F && data[1] == 0x8B))


class Message:
	var Type = 0
	var ID = 0
	var Route = ""
	var Data = PoolByteArray()
	var compressed = false
	var Err = false
	var SendAt = 0
	var Callback = null
