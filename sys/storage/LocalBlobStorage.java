package sys.storage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.HashMap;
import java.util.List;

import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;
import sys.storage.io.BufferedBlobReader;
import sys.storage.io.BufferedBlobWriter;

public class LocalBlobStorage implements BlobStorage {
	private static final int BLOCK_SIZE=512;

	//
	static Namenode namenode;
	static HashMap<String,DatanodeClient> datanodes;
	
	
	public static void main(String[] args) throws IOException {
		namenode = new NamenodeClient();
		datanodes = new HashMap();
		
		final int port = 9000 ;
		final InetAddress group = InetAddress.getByName( args[0] ) ;

		if( ! group.isMulticastAddress()) {
		    System.out.println( "Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
		}

		byte[] buf = new byte[65536];
		
		//Namenode connect
		byte[] data = "Namenode".getBytes();
		try(MulticastSocket socket = new MulticastSocket()) {
		    DatagramPacket request = new DatagramPacket( data, data.length, group, port ) ;
		    socket.send(request);
		    request = new DatagramPacket(buf,buf.length);
		    socket.receive(request);
		    
		}
		
		//Datanode connect
		data = "Datanode".getBytes();
		try(MulticastSocket socket = new MulticastSocket()) {
		    DatagramPacket request = new DatagramPacket( data, data.length, group, port );
		    socket.send(request);
		    request = new DatagramPacket(buf,buf.length);
		    socket.receive(request);
		}
	}

	@Override
	public List<String> listBlobs(String prefix) {
		return namenode.list(prefix);
	}

	@Override
	public void deleteBlobs(String prefix) {
		namenode.list( prefix ).forEach( blob -> {
			namenode.read( blob ).forEach( block -> {
				datanodes.get(blob).deleteBlock(block);
;				//datanodes[0].deleteBlock(block);
			});
		});
		namenode.delete(prefix);
	}

	@Override
	public BlobReader readBlob(String name) {
		return new BufferedBlobReader( name, namenode, datanodes.);
	}

	@Override
	public BlobWriter blobWriter(String name) {
		return new BufferedBlobWriter( name, namenode, datanodes, BLOCK_SIZE);
	}
}
