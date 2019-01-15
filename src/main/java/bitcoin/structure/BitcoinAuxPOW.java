package bitcoin.structure;

public class BitcoinAuxPOW {
	private int version;
	private BitcoinTransaction coinbaseTransaction;
	private byte[] parentBlockHeaderHash;
	private BitcoinAuxPOWBranch coinbaseBranch;
	private BitcoinAuxPOWBranch auxBlockChainBranch;
	private BitcoinAuxPOWBlockHeader parentBlockHeader;

	/*
	 * Creates an empy AuxPOW object in case the feature is not used (e.g. in the main Bitcoin blockchain)
	 * 
	 */
	public BitcoinAuxPOW() {
		this.version=0;
		this.coinbaseTransaction=null;
		this.coinbaseBranch=null;
		this.auxBlockChainBranch=null;
		this.parentBlockHeader=null;
	}
	
	public BitcoinAuxPOW(int version, BitcoinTransaction coinbaseTransaction, byte[] parentBlockHeaderHash, BitcoinAuxPOWBranch coinbaseBranch, BitcoinAuxPOWBranch auxBlockChainBranch, BitcoinAuxPOWBlockHeader parentBlockHeader) {
		this.version=version;
		this.coinbaseTransaction=coinbaseTransaction;
		this.parentBlockHeaderHash=parentBlockHeaderHash;
		this.coinbaseBranch=coinbaseBranch;
		this.auxBlockChainBranch=auxBlockChainBranch;
		this.parentBlockHeader=parentBlockHeader;
	}
	
	public int getVersion() {
		return version;
	}

	public BitcoinAuxPOWBranch getCoinbaseBranch() {
		return coinbaseBranch;
	}
	public void setCoinbaseBranch(BitcoinAuxPOWBranch coinbaseBranch) {
		this.coinbaseBranch = coinbaseBranch;
	}
	public BitcoinAuxPOWBranch getAuxBlockChainBranch() {
		return auxBlockChainBranch;
	}

	public BitcoinAuxPOWBlockHeader getParentBlockHeader() {
		return parentBlockHeader;
	}

	public BitcoinTransaction getCoinbaseTransaction() {
		return coinbaseTransaction;
	}

	public byte[] getParentBlockHeaderHash() {
		return parentBlockHeaderHash;
	}




}
