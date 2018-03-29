package com.faceoff;

import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;


import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

public class TransactionService {

    protected AerospikeClient client = null;

    String namespace = "test"; // test
    String setName = "transactions"; //transactions
    String id = null;
    String uid = null;
    int winings = 100;
    Map<String, String> map = new HashMap<String, String>();


    public TransactionService(AerospikeClient c){

        this.client = c;
        this.createTransaction();

    }


    public void createTransaction() throws AerospikeException{

        System.out.println("****** Create Transaction *******");

        createNTransaction(1);


    }
    public void createNTransaction(int n) throws AerospikeException{

        System.out.println("****** Create " + n + " Transaction *******");

        WritePolicy policy = getWritePolicy();

        for(int j = 1; j <= n; j++){

            //System.out.println("****** Create " + j + " Transaction *******");

            map = new HashMap<String, String>();
            map.put("name","Steve" + j);

            Key key = new Key(namespace,setName,j);

            Bin bin1 = new Bin("username","stevetest" + j);
            Bin bin2 = new Bin("password","stevepass" + j);
            Bin bin3 = new Bin("winings",winings);


            this.client.put(policy, key, bin1, bin2, bin3);

            //System.out.println("****** DONE Create " + j + " Transaction *******");

        }

    }

    public void create100Transactions() throws AerospikeException{

        System.out.println(GregorianCalendar.getInstance().toString());
        System.out.println("****** Create 1000000 Transaction *******");

        createNTransaction(1000000);

        System.out.println("****** DONE Create 1000000 Transaction *******");
        System.out.println(GregorianCalendar.getInstance().toString());


    }

    public Record readTransaction(int id) throws AerospikeException{

        System.out.println("****** Read Transaction " + id + " *******");

        WritePolicy policy = getWritePolicy();

        Key key = new Key(namespace,setName,"" + id);

        Record rec = this.client.get(policy, key);

        System.out.println("****** DONE Read Transaction *******");

        if(rec != null) {
            System.out.println(rec);
            return rec;
        }
        else{
            System.out.println("Unable to find record with that id");
            return null;
        }



    }

    public void readNTransaction(int idStart, int idEnd) throws Exception,AerospikeException{

        if(idEnd < idStart){
            throw new Exception("End index less than start index, " + idStart + ":" + idEnd);
        }
        System.out.println("****** Read start index = " + idStart + ": end index = " + idEnd + " Transaction *******");

        for( int i = idStart; i <= idEnd; i++){
            readTransaction(i);
        }

        System.out.println("****** DONE Read N Transaction *******");

    }

    public void deleteNTransaction(int idStart, int idEnd) throws Exception,AerospikeException{

        if(idEnd < idStart){
            throw new Exception("End index less than start index, " + idStart + ":" + idEnd);
        }
        System.out.println("****** Read start index = " + idStart + ": end index = " + idEnd + " Transaction *******");

        for( int i = idStart; i <= idEnd; i++){
            deleteTransaction(i);
        }

        System.out.println("****** DONE Read N Transaction *******");

    }


    public void deleteTransaction(int id) throws AerospikeException{

        System.out.println("****** Delete Transaction *******");

        WritePolicy policy = getWritePolicy();

        Key key = new Key(namespace,setName,"" + id);

        Record rec = this.client.get(policy, key);

        if(rec != null){

            System.out.println("Found transaction to delete ... " + rec.toString());

            client.delete(policy, key);

        }

        System.out.println(rec);
    }



    public WritePolicy getWritePolicy(){

        WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        return policy;

    }

    public void updateTransaction(int id) throws AerospikeException{

        System.out.println("****** Update Transaction *******");

        Record rec = readTransaction(id);
        Key key = new Key(namespace,setName,"" + id);

        if(rec == null){
            System.out.println("Unable to locate Transaction ...*");
            return;
        }
        else{
            Bin bin3 = new Bin("winings",winings + 100);
            client.append(getWritePolicy(), key, bin3);

            Record rec2 = this.client.get(getWritePolicy(), key);

            System.out.println("Updated transaction ... " + rec2.toString());
        }


    }

    public void appendTransaction(int id) throws AerospikeException{

        System.out.println("****** Append Transaction *******");

        Record rec = readTransaction(id);
        Key key = new Key(namespace,setName,"" + id);

        if(rec == null){
            System.out.println("Unable to locate Transaction ...*");
            return;
        }
        else{
            Bin bin = new Bin("updated",GregorianCalendar.getInstance().toString());
            client.append(getWritePolicy(), key, bin);

            Record rec2 = this.client.get(getWritePolicy(), key);

            System.out.println("Appended transaction ... " + rec2.toString());
        }


    }

    public void sumWinningsTransaction() throws AerospikeException {

        System.out.println("****** SUM WINNING TransactionS *******");

        try {
            runSumQuery(this.client,null, null, "winings");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("ERROR:  " + e.toString());
        }

        System.out.println("****** DONE SUM WINNING Transaction *******");


    }

    public void countWinningsTransaction() throws AerospikeException {

        System.out.println(GregorianCalendar.getInstance().toString());
        System.out.println("****** COUNT WINNING TransactionS *******");

        try {
            runCountsQuery(this.client,null, null, "winings");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("ERROR:  " + e.toString());
        }

        System.out.println("****** DONE COUNT WINNING Transaction *******");
        System.out.println(GregorianCalendar.getInstance().toString());

    }



    private int runCountsQuery(
            AerospikeClient client,
            Map params,
            String indexName,
            String binName
    ) throws Exception {

        int begin = 99;
        int end = 102;

        System.out.println("Query for: ns=%s set=%s index=%s bin=%s >= %s <= %s");
                //params.namespace, params.set, indexName, binName, begin, end);

        IndexTask task = client.createIndex(null, namespace, setName,
                "idx_foo_bar_baz", binName, IndexType.NUMERIC);

        task.waitTillComplete();


        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(setName);
        stmt.setBinNames(binName);
        stmt.setFilters(Filter.range(binName, begin, end));

        RecordSet rs = client.query(null, stmt);
        int count = 0;
        try {

            while (rs.next()) {

                Key key = rs.getKey();
                Record record = rs.getRecord();
                int result = record.getInt(binName);
                System.out.println(result);

                System.out.println("Record found: ns=%s set=%s bin=%s digest=%s value=%s");
                       // key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);

                count++;
            }

            if (count != 10000) {
                System.out.println("Query count mismatch. Expected 5. Received " + count);
            }
        }
        finally {
            rs.close();
        }

        return count;
    }

    private int runSumQuery(
            AerospikeClient client,
            Map params,
            String indexName,
            String binName
    ) throws Exception {

        int begin = 99;
        int end = 102;

        //System.out.println("Query for: ns=%s set=%s index=%s bin=%s >= %s <= %s");
        //params.namespace, params.set, indexName, binName, begin, end);

        IndexTask task = client.createIndex(null, namespace, setName,
                "idx_foo_bar_baz", binName, IndexType.NUMERIC);

        task.waitTillComplete();


        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(setName);
        stmt.setBinNames(binName);
        stmt.setFilters(Filter.range(binName, begin, end));

        RecordSet rs = client.query(null, stmt);
        int sum = 0;
        try {

            while (rs.next()) {

                Key key = rs.getKey();
                Record record = rs.getRecord();
                int result = record.getInt(binName);
                //System.out.println(result);

                //System.out.println("Record found: ns=%s set=%s bin=%s digest=%s value=%s");
                // key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);

                sum = sum + result;
            }

            if (sum != 10000000) {
                System.out.println("Query count mismatch. Expected 5. Received " + sum);
            }

            System.out.println("Query sum = " + sum);
        }
        finally {
            rs.close();
        }

        return sum;
    }


}
