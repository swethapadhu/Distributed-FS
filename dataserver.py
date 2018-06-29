#!/usr/bin/env python

"""
Author: David Wolinsky
Version: 0.03

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => Binary
      print rv.data => "value"
  put(base64 key, base64 value)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"))
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable

Changelog:
    0.03 - Modified to remove timeout mechanism for data.
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest, shelve
from datetime import datetime, timedelta
from xmlrpclib import Binary
from random import randint

# Presents a HT interface
class SimpleHT:
  def __init__(self):
    self.data = {}
    self.datareplica = {}
    self.ID = int(sys.argv[1])
    current = int(sys.argv[self.ID+2])
    
    if self.ID == (len(sys.argv[2:])-1) :
    	next = sys.argv[2]
    else:
    	next = sys.argv[self.ID+3]
    
    if self.ID == 0 :
    	previous = sys.argv[-1]
    else:
    	previous = sys.argv[self.ID+1]
    		
    nextserve = xmlrpclib.ServerProxy('http://localhost: %s' % next)
    prevserve = xmlrpclib.ServerProxy('http://localhost: %s' % previous)
    
    d=shelve.open(('datastore'+str(self.ID)))
    klist=d.keys()
    c1=0
    c2=0
    if klist == []:
    	try:
    		c1=nextserve.count()
    	except:
		self.data ={}
		
	try:
    		c2=prevserve.count()
    	except:
		self.datareplica ={}
	
    	if c1 > 0:
    		nlist=nextserve.listingreplica()
    		for key in nlist:
    			d = shelve.open(('datastore'+str(self.ID)))
    			d[str(key)] = nextserve.getreplica(key)
    			self.data[key]= d[str(key)]
    			d.close()
    				
    	if c2 > 0:
    		plist=prevserve.listing()
    		for key in plist:
    			d = shelve.open(('datastore'+str(self.ID)))
    			d[('r'+str(key))] = prevserve.get(key)
    			self.datareplica[key]=d[('r'+str(key))]
    		
    else:
    	for key in klist:
    		if str(key[0]) == 'r':
    			self.datareplica[key[1:]]=d[key]
    		else:
    			self.data[key]=d[key]
    d.close()
    
    			
  def count(self):
    return (len(self.data)+len(self.datareplica))

  # Retrieve something from the HT
  def get(self, key):
    # Default return value
    rv = {}
    # If the key is in the data structure, return properly formated results
    if key in self.data:
      rv = self.data[key]
    return rv

  def getreplica(self, key):
    # Default return value
    rv = {}
    # If the key is in the data structure, return properly formated results
    if key in self.datareplica:
      rv = self.datareplica[key]
    return rv

  # Insert something into the HT
  def put(self, key, value):
    # Remove expired entries
    self.data[key] = value
    d = shelve.open(('datastore'+str(self.ID)))
    d[str(key)] = value  
    d.close()
    return True
    
  def putreplica(self, key, value):
    # Remove expired entries
    self.datareplica[key] = value
    d = shelve.open(('datastore'+str(self.ID)))
    d[('r'+str(key))] = value
    d.close()
    return True

  def clear(self):
    self.data.clear()
    self.datareplica.clear()
    d=shelve.open(('datastore'+str(self.ID)))
    klist=d.keys()
    for key in klist:
    	del d[key] 
    return True
    
  def remove(self, key):
    if key in self.data:
    	del self.data[key]
    	d = shelve.open(('datastore'+str(self.ID)))
    	del d[str(key)]
    	return True
    else:
    	return False

  def removereplica(self, key):
    if key in self.datareplica:
    	del self.datareplica[key]
    	d = shelve.open(('datastore'+str(self.ID)))
	del d[('r'+str(key))]
    	return True
    else:
    	return False
    
  def listing(self):
    t=self.data
    return t.keys()

  def listingreplica(self):
    t=self.datareplica
    return t.keys()
    
  def corrupt(self,path):
  	path=str(path)
  	i1=0
  	i2=0
  	for key in self.data:
  		if key[:len(path)] == path:
  			t=self.data[key]	 
  			r=randint(0,len(t[0])-1)
  			if (ord(t[0][r]) > 31) and (ord(t[0][r]) < 127):
  				t[0]=str(t[0][:r])+chr(ord(t[0][r])+randint(1,9))+str(t[0][r+1:])
  			else:
  				t[0]=str(t[0][:r])+str('!')+str(t[0][r+1:])
  			print ('Data block %s of file %s corrupted' % (key[len(path):],path))
  			i1=1
  			break
	
	for key in self.datareplica:
  		if key[:len(path)] == path:
  			t=self.datareplica[key]	 
  			t[1]=t[1]+randint(1,9)
  			print ('Data block %s of file %s corrupted' % (key[len(path):],path))
  			i2=1
  			break
  			
  	if i1+i2 !=0:
  		return True
  	else:
  		return False

  # Load contents from a file
  def read_file(self, filename):
    f = open(filename, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

def main():
  '''optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  ol={}
  for k,v in optlist:
    ol[k] = v

  port = 51234
  if "--port" in ol:
    port = int(ol["--port"])
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  serve(port)'''
  sht = SimpleHT()
  port = int(sys.argv[int(sys.argv[1])+2])
  serve(port,sht)

# Start the xmlrpc server
def serve(port,sht):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  
  file_server.register_function(sht.count)
  file_server.register_function(sht.get)
  file_server.register_function(sht.getreplica)
  file_server.register_function(sht.put)
  file_server.register_function(sht.putreplica)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.register_function(sht.clear)
  file_server.register_function(sht.remove)
  file_server.register_function(sht.removereplica)
  file_server.register_function(sht.listing)
  file_server.register_function(sht.listingreplica)
  file_server.register_function(sht.corrupt)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port,sht)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(51234, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
  main()
