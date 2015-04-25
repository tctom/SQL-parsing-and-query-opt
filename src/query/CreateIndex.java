package query;

import heap.HeapFile;
import index.HashIndex;
import global.Minibase;
import global.SearchKey;
import relop.FileScan;
import relop.Schema;
import relop.Tuple;
import parser.AST_CreateIndex;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {
	// index file name
	protected String indexName;

	// table name
	protected String tableName;

	// indexed column name
	protected String colName;

	// schema
	protected Schema schema;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if index already exists or table/column invalid
	 */
	public CreateIndex(AST_CreateIndex tree) throws QueryException {
		indexName = tree.getFileName();
		tableName = tree.getIxTable();
		colName = tree.getIxColumn();
		QueryCheck.tableExists(tableName);
		QueryCheck.fileNotExists(indexName);
		schema = Minibase.SystemCatalog.getSchema(tableName);
		QueryCheck.columnExists(schema, colName);
		// checks if the column has already been indexed
		IndexDesc[] indexArr = Minibase.SystemCatalog.getIndexes(tableName);
		for (int i = 0; i < indexArr.length; i++) {
			if (indexArr[i].columnName.equals(colName)) { // ????????????????/
				throw new QueryException("index already exists");
			}
		}
	} // public CreateIndex(AST_CreateIndex tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		HashIndex hindex = new HashIndex(indexName);
		// scan heap file to build the hash index
		FileScan fs = new FileScan(schema, new HeapFile(tableName));
		int fn = schema.fieldNumber(colName);
		while (fs.hasNext()) {
			Tuple t = fs.getNext();
			hindex.insertEntry(new SearchKey(t.getField(fn)), fs.getLastRID());
		}
		fs.close();
		Minibase.SystemCatalog.createIndex(indexName, tableName, colName);
		// print the output message
		System.out.println("Index " + indexName + " created on table "
				+ tableName + " at column " + colName);
	} // public void execute()

} // class CreateIndex implements Plan
