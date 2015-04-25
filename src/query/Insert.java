package query;

import index.HashIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {
	protected String tableName;
	protected Object[] values;
	Schema schema;
	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if table doesn't exists or values are invalid
	 */
	public Insert(AST_Insert tree) throws QueryException {
		tableName = tree.getFileName();
		schema = Minibase.SystemCatalog.getSchema(tableName);
		values = tree.getValues();
		QueryCheck.tableExists(tableName);
		QueryCheck.insertValues(schema, values);
	} // public Insert(AST_Insert tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		HeapFile hf = new HeapFile(tableName);
		Tuple t = new Tuple(schema, values);
		RID rid = hf.insertRecord(t.getData());
		
		// update all indexes
		IndexDesc[] indexArr = Minibase.SystemCatalog.getIndexes(tableName);
		for (int i=0; i<indexArr.length; i++){
			String indexName = indexArr[i].indexName;
			String colName = indexArr[i].columnName;
			HashIndex hIndex = new HashIndex(indexName);
			hIndex.insertEntry(new SearchKey(t.getField(colName)), rid);
		}
		// print the output message
		System.out.println("1 row inserted to table " + tableName);

	} // public void execute()

} // class Insert implements Plan
