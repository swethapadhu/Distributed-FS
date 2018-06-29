#!/usr/bin/env python


import logging, xmlrpclib, pickle

from xmlrpclib import Binary
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

blksize=512

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):

    def __init__(self):
        self.fd = 0
        now = time()                                                
	self.mserve = xmlrpclib.ServerProxy('http://localhost: %s' % argv[2])
	self.dserve = [xmlrpclib.ServerProxy('http://localhost: %s' % i) for i in argv[3:]]
	self.mserve.clear()
        for i in self.dserve:
            i.clear()
        d0 =self.dserve[0]
	self.dserve.append(d0)
	self.mserve.put('/', dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2, child=[]))
    	
    def getdata(self, blknum, path):
	while True:
		try:
			temp=self.dserve[(blknum)%(len(self.dserve)-1)].get(str(path)+str(blknum))
			break
		except:
			continue
	return temp
				
    def getdatareplica(self, blknum, path):
	while True:
		try:
			temp=self.dserve[(blknum+1)%(len(self.dserve)-1)].getreplica(str(path)+str(blknum))
			break
		except:
			continue
	return temp
    
    def chksum(self,db):
    	return sum((j+1)*ord(db[j]) for j in range(len(db)))
    
    def putdata(self, blknum, path, datablk):
    	while True:
		try:
			self.dserve[(blknum)%(len(self.dserve)-1)].put(str(path)+str(blknum), [datablk, self.chksum(datablk)])
			break
		except:
			continue
	while True:
		try:
			self.dserve[(blknum+1)%(len(self.dserve)-1)].putreplica(str(path)+str(blknum), [datablk, self.chksum(datablk)])
			break
		except:
			continue
	return 0
	    
    def putdatablks(self,datablks,path,nblks):
    	for i in range(len(datablks)):
    		self.putdata(nblks+i, path, datablks[i])
    
    def removedata(self, blknum, path):
    	while True: 
		try:
			self.dserve[(blknum)%(len(self.dserve)-1)].remove(str(path)+str(blknum))
			break
		except:
			continue
	while True: 
		try:
			self.dserve[(blknum+1)%(len(self.dserve)-1)].removereplica(str(path)+str(blknum))
			break
		except:
			continue
	return 0
    
    def lastbefore(self, path):
	list_splitpath = path.split('/')
	last = list_splitpath[-1]
	paths=str(path)      
        lastbefore=paths[:(-1-len(last))]
        if lastbefore=='':
        	lastbefore='/'	
	return last, lastbefore
    
    def chmod(self, path, mode):
    	m=self.mserve
        rv=m.get(path)
        rv['st_mode'] &= 0770000
        rv['st_mode'] |= mode
      	m.put(path,rv)
        return 0

    def chown(self, path, uid, gid): 
        m=self.mserve
        rv = m.get(path)
	rv['st_uid'] = uid
        rv['st_gid'] = gid
        m.put(path,rv)

    def create(self, path, mode):
 	m=self.mserve
        last,lastbefore=self.lastbefore(path)
        rv=m.get(lastbefore)
        rv['child'].append(last)
        rv['st_nlink'] += 1
      	m.put(path, dict(st_mode=(S_IFREG | mode), st_nlink=1,
                            st_size=0, st_ctime=time(), st_mtime=time(),
                            st_atime=time()))
      	m.put(lastbefore, rv)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):   
        try:
        	m=self.mserve
       		rv= m.get(path)	
        except:     	    
            	raise FuseOSError(ENOENT)
        return {attr:rv[attr] for attr in rv.keys() if attr != 'child'}
	
    def getxattr(self, path, name, position=0):
        m=self.mserve
        rv=m.get(path)
        attrs = rv.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''      

    def listxattr(self, path):
	m=self.mserve
        rv=m.get(path)
        attrs = rv.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        m=self.mserve
       	last,lastbefore=self.lastbefore(path)
       	rv=m.get(lastbefore)
       	rv['child'].append(last)
      	rv['st_nlink'] += 1
      	m.put(path, dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                            st_size=0, st_ctime=time(), st_mtime=time(),
                            st_atime=time(),child=[]))
      	m.put(lastbefore, rv)

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
	m=self.mserve
	rv=m.get(path)	
	d=self.dserve
	nblks=((rv['st_size']-1)//blksize)+1
	read_data=[]
	for i in range(nblks):
		while True :
			try:
				db1=d[i%(len(d)-1)].get(str(path)+str(i))
				if db1[1]==self.chksum(db1[0]):
					read_data.append(db1[0])
					try:
						db2=d[(i+1)%(len(d)-1)].getreplica(str(path)+str(i))
						if db2[1]!=self.chksum(db2[0]):
							d[(i+1)%(len(d)-1)].putreplica(str(path)+str(i),db1)
							print ('Data block %d of file %s corrected' % (i,path))
						break
					except:
						break
				else:
					try:
						db2=d[(i+1)%(len(d)-1)].getreplica(str(path)+str(i))
						if db2[1]==self.chksum(db2[0]):
							read_data.append(db2[0])
							d[i%(len(d)-1)].put(str(path)+str(i),db2)
							print ('Data block %d of file %s corrected' % (i,path))
							break
					except:				
						continue
			except: 
				try:
					db2=d[(i+1)%(len(d)-1)].getreplica(str(path)+str(i))
					if db2[1]==self.chksum(db2[0]):
						read_data.append(db2[0])
						break
				except:
					continue
	a=''.join(read_data)
	return a
      	
    def readdir(self, path, fh):
	m=self.mserve
        rv=m.get(path)
        return rv['child']
	
    def readlink(self, path):
    	m=self.mserve
	rv=m.get(path)
	d=self.dserve
	nblks=((rv['st_size']-1)//blksize)+1
	read_data=[]
	for i in range(nblks):
		temp=self.getdata(i,path)	
		read_data.append(temp[0])
	a=''.join(read_data)
	return a
	
    def removexattr(self, path, name):
	m=self.mserve
        rv=m.get(path)
        attrs = rv.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass  
    
    def rename(self, old, new):
	m=self.mserve
       	last_old,lastbeforeold=self.lastbefore(old)
	last_new,lastbeforenew=self.lastbefore(new)
        rv=m.get(old)
        m.remove(old)
        m.put(new,rv)
        rv_old_before=m.get(lastbeforeold)
        rv_old_before['st_nlink'] -=1 
        rv_old_before['child'].remove(last_old) 
        m.put(lastbeforeold,rv_old_before)
        rv_new_before=m.get(lastbeforenew)
        rv_new_before['st_nlink'] +=1
        rv_new_before['child'].append(last_new)        
	m.put(lastbeforenew,rv_new_before)
	d=self.dserve
	rv=m.get(new)
	nblks=((rv['st_size']-1)//blksize)+1
	for i in range(nblks):
		temp=self.getdata(i,old)
		self.removedata(i,old)
		self.putdata(i,new,temp[0])	

    def rmdir(self, path):
	m=self.mserve
	keys_server=m.listing()
	for item in keys_server:
		if item != path:
			if str(item[:len(path)])==str(path):
				rv=m.get(item)
				if 'child' not in rv:
					self.unlink(item)
				m.remove(item)
	m.remove(path)
        last,lastbefore=self.lastbefore(path)        	
        rv_before=m.get(lastbefore)
	rv_before['st_nlink'] -= 1
	rv_before['child'].remove(last)
	m.put(lastbefore, rv_before)
        
    def setxattr(self, path, name, value, options, position=0):
        m=self.mserve
        rv=m.get(path)
        attrs = rv.setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        m=self.mserve
        last,lastbefore = self.lastbefore(target)
        rv_before=m.get(lastbefore)
        rv_before['child'].append(last)
        m.put(lastbefore,rv_before)     
        m.put(target,dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                st_size=len(source)))
        datablks = [source[i:i+blksize] for i in range(0, len(source), blksize)]
        self.putdatablks(datablks, target, 0)

    def truncate(self, path, length, fh=None):
	m=self.mserve
	rv=m.get(path)
	rv['st_size']=length if length > rv['st_size'] else rv['st_size']
	m.put(path,rv)
	        
    def unlink(self, path):
	m=self.mserve                	
        last,lastbefore=self.lastbefore(path)
        rv=m.get(lastbefore)
        rv['child'].remove(last)
        rv['st_nlink'] -= 1
        m.put(lastbefore, rv)
        rv=m.get(path)	
	m.remove(path)
	d=self.dserve
	nblks=((rv['st_size']-1)//blksize)+1
	for i in range(nblks):
		self.removedata(i, path)

    def utimens(self, path, times=None):
	m=self.mserve        
	rv=m.get(path)
	now = time()
        atime, mtime = times if times else (now, now)
        rv['st_atime'] = atime
        rv['st_mtime'] = mtime
        m.put(path,rv)

    def write(self, path, data, offset, fh):        
        m=self.mserve
	rv=m.get(path)                     		 
        d=self.dserve
	nblks=((rv['st_size']-1)//blksize)+1    		
	rem = rv['st_size']%blksize
	if rv['st_size']==offset:
		if offset==0:
			datablks=[data[i:i+blksize] for i in range(0, len(data),blksize)]
			self.putdatablks(datablks,path,nblks)
		else:	
			if rem > 0:
				x1=self.getdata(nblks-1, path)
				x=x1[0]+data[:blksize-rem]
				self.putdata(nblks-1, path, x)
				b=data[blksize-rem:]
				if b!= '':
					datablks = [b[i:i+blksize] for i in range(0,len(b),blksize)]
					self.putdatablks(datablks,path,nblks)
			else:
				datablks=[data[i:i+blksize] for i in range(0,len(data),blksize)]
				self.putdatablks(datablks,path,nblks)
		rv['st_size']=rv['st_size']+len(data)
	else:
		newblks=((len(data)-1)//blksize)+1
		datablks=[data[i:i+blksize] for i in range(0,len(data),blksize)]
		x1=self.getdata(newblks-1, path)
		datablks[-1]=str(datablks[-1])+str(x1[0][len(datablks[-1]):])
		self.putdatablks(datablks, path, 0)		
		rv['st_size']=len(data) if len(data) > rv['st_size'] else rv['st_size']
	m.put(path, rv)
	return len(data)	
		       
if __name__ == '__main__':
    if len(argv) < 4:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True)
