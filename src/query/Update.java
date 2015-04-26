package query;

import index.HashIndex;

import java.util.ArrayList;

import global.AttrType;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Update;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {

	private String file;
	private Schema schema;
	private Predicate[][] preds;
	private Object[] values;
	private String[] columns;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if invalid column names, values, or pedicates
	 */
	public Update(AST_Update tree) throws QueryException {
		file = tree.getFileName();
		preds = tree.getPredicates();
		schema = Minibase.SystemCatalog.getSchema(file);
		values = tree.getValues();
		columns = tree.getColumns();
		// check columns
		for (String c : columns) {
			QueryCheck.columnExists(schema, c);
		}
		// check value class
		int[] fn = new int[values.length];
		for (int i = 0; i < values.length; i++) {
			fn[i] = schema.fieldNumber(columns[i]);
		}
		QueryCheck.updateValues(schema, fn, values);

		QueryCheck.tableExists(file);
		QueryCheck.predicates(schema, preds);

	} // public Update(AST_Update tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {

		HeapFile f = new HeapFile(file);
		FileScan fs = new FileScan(schema, f);
		while (fs.hasNext()) { // consider pred in same list as AND
			Tuple t = fs.getNext();
			boolean pass = true;
			for (int i = 0; i < preds.length; i++) {
				boolean localPass = false;
				for (Predicate p : preds[i]) {
					localPass |= p.evaluate(t);
				}
				pass &= localPass;
			}

			if (pass || preds.length == 0) {// tuple match
				Tuple updateT = new Tuple(schema, t.getData());
				for (int i = 0; i < columns.length; i++) {
					updateT.setField(columns[i], values[i]);
				}

				IndexDesc[] indexes = Minibase.SystemCatalog.getIndexes(file);
				for (IndexDesc ind : indexes) {
					String indexName = ind.indexName;
					String colName = ind.columnName;
					HashIndex index = new HashIndex(indexName);
					SearchKey oldKey = new SearchKey(t.getField(colName));
					SearchKey newKey = new SearchKey(updateT.getField(colName));
					index.deleteEntry(oldKey, fs.getLastRID());
					index.insertEntry(newKey, fs.getLastRID());
				}

				f.updateRecord(fs.getLastRID(), updateT.getData());
			}
		}
		fs.close();
		/*
		 * fs = new FileScan(schema,f); while(fs.hasNext()){ Tuple t =
		 * fs.getNext(); Object[] allF = t.getAllFields();
		 * System.out.print((Integer)allF[0]+",");
		 * System.out.print((String)allF[1]+",");
		 * System.out.print((Float)allF[2]+","); System.out.println(); }
		 */

		// public void execute()
	} // public void execute()

} // class Update implements Plan
