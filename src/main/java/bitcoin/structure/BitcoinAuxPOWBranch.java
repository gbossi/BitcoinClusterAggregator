package bitcoin.structure;

import java.util.List;

public class BitcoinAuxPOWBranch {
	private byte[] numberOfLinks;
	private List<byte[]> links;
	private byte[] branchSideBitmask;
	
	public BitcoinAuxPOWBranch(byte[] numberOfLinks, List<byte[]> links, byte[] branchSideBitmask) {
		this.numberOfLinks=numberOfLinks;
		this.links=links;
		this.branchSideBitmask=branchSideBitmask;
		
	}
	
	public byte[] getNumberOfLinks() {
		return this.numberOfLinks;
	}
	
	public List<byte[]> getLinks() {
		return this.links;
	}
	
	public byte[] getBranchSideBitmask() {
		return this.branchSideBitmask;
	}
}
