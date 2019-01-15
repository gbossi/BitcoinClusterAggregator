package bitcoin.structure;

public class BitcoinAuxPOWBlockHeader {
	private int version;
	private byte[] previousBlockHash;
	private byte[] merkleRoot;
	private int time;
	private byte[] bits;
	private int nonce;
	
	
	public BitcoinAuxPOWBlockHeader(int version, byte[] previousBlockHash, byte[] merkleRoot, int time, byte[] bits, int nonce) {
		this.version=version;
		this.previousBlockHash=previousBlockHash;
		this.merkleRoot=merkleRoot;
		this.time=time;
		this.bits=bits;
		this.nonce=nonce;
	}
	
	public int getVersion() {
		return version;
	}

	public byte[] getPreviousBlockHash() {
		return previousBlockHash;
	}

	public byte[] getMerkleRoot() {
		return merkleRoot;
	}

	public int getTime() {
		return time;
	}

	public byte[] getBits() {
		return bits;
	}

	public int getNonce() {
		return nonce;
	}

}
