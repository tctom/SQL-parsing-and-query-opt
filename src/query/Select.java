package query;

import java.util.ArrayList;

import global.AttrOperator;
import global.AttrType;
import global.Minibase;
import global.SortKey;
import heap.HeapFile;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {
	// projection list
	String[] projs;

	// joining tables
	String[] tables;

	// predicate list
	Predicate[][] preds;

	// columns to do sorting
	SortKey[] sortCols;

	// Filescan on tables
	ArrayList<Iterator> tableIters;

	// final schema
	Schema finalSch;
	// final iterator
	Iterator finalIter;

	// Keep a record of the pushed selection, we can do other selections on the
	// fly
	boolean[] pushed;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if validation fails
	 */
	public Select(AST_Select tree) throws QueryException {
		projs = tree.getColumns();
		tables = tree.getTables();
		sortCols = tree.getOrders();
		preds = tree.getPredicates();
		pushed = new boolean[preds.length];
		// initialize pushed as all false
		for (int i = 0; i < preds.length; i++) {
			pushed[i] = false;
		}

		// check if all tables exist
		for (String tablesName : tables) {
			QueryCheck.tableExists(tablesName);

		}

		// join schema
		Schema new_schema = new Schema(0); // create an empty schema
		for (String tableName : tables) {
			new_schema = Schema.join(new_schema,
					Minibase.SystemCatalog.getSchema(tableName));
		}

		// check if projs are valid
		for (String proj : projs) {
			if (new_schema.fieldNumber(proj) < 0)
				throw new QueryException("Projection " + proj
						+ " does not exist!");
		}

		// check predicates
		for (Predicate[] pa : preds) {
			for (Predicate p : pa) {
				if (!p.validate(new_schema))
					throw new QueryException("Invalid Predication: "
							+ p.toString());
			}
		}

		// build initial iterator as filescan for each table
		tableIters = new ArrayList(tables.length);
		// new FileScan[tables.length];
		for (int i = 0; i < tables.length; i++) {
			tableIters.add(new FileScan(Minibase.SystemCatalog
					.getSchema(tables[i]), new HeapFile(tables[i])));
		}

		// find selections that we can push
		ArrayList<ArrayList<Predicate[]>> pushables = new ArrayList<ArrayList<Predicate[]>>(
				tables.length);
		// new ArrayList[tables.length];
		for (int i = 0; i < tables.length; i++) {
			ArrayList<Predicate[]> tmp = new ArrayList();
			for (int j = 0; j < preds.length; j++) {

				Predicate[] CNF = preds[j];
				boolean valid = true;
				for (Predicate p : CNF) {
					valid = valid
							&& p.validate(Minibase.SystemCatalog
									.getSchema(tables[i]));
					if (!valid)
						break;
				}
				if (valid) {
					pushed[j] = true;
					tmp.add(CNF);
					// pushables.get(i).add(CNF);
				}
			}
			pushables.add(tmp);
		}

		// push selection (substitute filescan with selection)
		for (int i = 0; i < tables.length; i++) {
			if (pushables.get(i).size() > 0) {
				for (int j = 0; j < pushables.get(i).size(); j++) {
					Predicate[] CNF = pushables.get(i).get(j);
					tableIters.set(i, (new Selection(tableIters.get(i), CNF)));
				}
			}
		}

		// start joins ordering
		finalSch = Minibase.SystemCatalog.getSchema(tables[0]);
		finalIter = tableIters.get(0);

		// tableIters[0].getSchema(); ??????????????
		// only consider joining when there are more than 2 tables
		if (tables.length >= 2) {
			for (int i = 1; i < tables.length; i++) {
				/* The previous table is smaller */
				if (getRecNum(tables[i - 1]) < getRecNum(tables[i])) {
					/* smaller table as outer */
					finalIter = new SimpleJoin(finalIter, tableIters.get(i));
					finalSch = Schema.join(finalSch,
							Minibase.SystemCatalog.getSchema(tables[i]));
				} else {
					finalIter = new SimpleJoin(tableIters.get(i), finalIter);
					finalSch = Schema.join(
							Minibase.SystemCatalog.getSchema(tables[i]),
							finalSch);
				}
			}
		}

		// selection on the fly for all remaining predicates
		for (int i = 0; i < preds.length; i++) {
			if (!pushed[i]) { // if not pushed, handle it
				finalIter = new Selection(finalIter, preds[i]);
			}
		}
		// Projection
		// build int cols
		if (projs.length > 0) { // only consider projection when there is at
								// least one column
			Integer[] cols = new Integer[projs.length];
			for (int i = 0; i < projs.length; i++) {
				cols[i] = finalSch.fieldNumber(projs[i]);
//				System.out.println(projs[i]);
//				System.out.println(cols[i]);
			}

			finalIter = new Projection(finalIter, cols);
		}

	} // public Select(AST_Select tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		finalIter.execute();
		finalIter.close();
		for (Iterator i : tableIters) {
			i.close();
		}
		// print the output message
		System.out.println("Selection implemented");
	} // public void execute()

	private int getRecNum(String tableName) {
		FileScan fs = new FileScan(Minibase.SystemCatalog.s_rel,
				Minibase.SystemCatalog.f_rel);
		Predicate p = new Predicate(AttrOperator.EQ, AttrType.COLNAME,
				"relName", AttrType.STRING, tableName);
		Selection s = new Selection(fs, p);
		s.hasNext();
		Tuple t = s.getNext();
		return ((Integer) t.getField("recCount")).intValue();
	}

} // class Select implements Plan
