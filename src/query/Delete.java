package query;

import java.util.ArrayList;

import global.Minibase;
import global.RID;
import global.SearchKey;
import parser.AST_Delete;
import index.HashIndex;
import global.*;
import heap.HeapFile;
import parser.AST_Insert;
import relop.*;
/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   * 
   */
	String file;
	Schema schema;
	Predicate[][] preds;
  
	public Delete(AST_Delete tree) throws QueryException {
		file = tree.getFileName();
		preds = tree.getPredicates();
		schema = Minibase.SystemCatalog.getSchema(file);
		QueryCheck.tableExists(file);
		QueryCheck.predicates(schema, preds);
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  	HeapFile f = new HeapFile(file);
	  	FileScan fs = new FileScan(schema,f);
	  	ArrayList<RID> delIDs = new ArrayList<RID>();
	  	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
	  	while(fs.hasNext()){ //consider pred in same list as AND 
	  		Tuple t = fs.getNext();
	  		boolean pass = true;
	  		for(int i=0; i<preds.length;i++){
	  			boolean localPass = false;
	  			for(Predicate p : preds[i]){
	  				localPass |= p.evaluate(t);
	  			}
	  			pass &= localPass;	
	  		}
	  		
	  		if(pass || preds.length==0){//tuple match
	  			delIDs.add(fs.getLastRID());	
	  			tuples.add(t);
	  		}
	  	}
	  	fs.close();
	  	for(int i=0;i<tuples.size();i++){ 
	  		
	  		
	  		//RID rid = f.insertRecord(tuple.getData());
	  		RID rid = delIDs.get(i);
	  		Tuple tuple = tuples.get(i);
	  		// update indexes of this table
	  		IndexDesc[] indexes = Minibase.SystemCatalog.getIndexes(file);
	  		for (IndexDesc ind : indexes)
	  		{
				String indexName = ind.indexName;
				String colName = ind.columnName;
				HashIndex index = new HashIndex(indexName);
				SearchKey key = new SearchKey(tuple.getField(colName));
				index.deleteEntry(key, rid);
			}
	  		
	  		f.deleteRecord(rid);
	  		// print the output message
	  		System.out.println("1 rows deleted.");
	  	}
	  	
	    /*fs = new FileScan(schema,f);
	    while(fs.hasNext()){//????????? why this shit can has next after i delete all.
	    	Tuple t = fs.getNext();
	    	Object[] allF = t.getAllFields();
	    	System.out.print((Integer)allF[0]+",");
	    	System.out.print((String)allF[1]+",");
	    	System.out.print((Float)allF[2]+",");
	    	System.out.println();
	    }*/
	    fs.close();

    // print the output message
    //System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Delete implements Plan
