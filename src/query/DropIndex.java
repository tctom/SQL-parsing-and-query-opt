package query;

import global.Minibase;
import index.HashIndex;
import parser.AST_DropIndex;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {
	protected String indexName;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if index doesn't exist
	 */
	public DropIndex(AST_DropIndex tree) throws QueryException {
		indexName = tree.getFileName();
		// check if there exists such a index
		QueryCheck.indexExists(indexName);
	} // public DropIndex(AST_DropIndex tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		new HashIndex(indexName).deleteFile();
		Minibase.SystemCatalog.dropIndex(indexName);
		// print the output message
		System.out.println("Index " + indexName + "dropped.");
	} // public void execute()

} // class DropIndex implements Plan
