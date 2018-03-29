package com.faceoff;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.ClientPolicy;

public class Main {

    protected AerospikeClient client = null;
    protected String seedHost = "172.28.128.3";
    protected int port = 3000;


    public static void main(String args[]){

        ClientPolicy cPolicy = new ClientPolicy();
        cPolicy.timeout = 500;

        Main main = new Main();
        main.init(cPolicy);
        main.work();


    }

    public void init(ClientPolicy cPolicy){
        this.client = new AerospikeClient(cPolicy, this.seedHost, this.port);
    }

    protected void finalize() throws Throwable {
        if (this.client != null){
            this.client.close();
        }
    };

    protected void work(){


        try {
            System.out.printf("INFO: Connecting to Aerospike cluster...");

            // Establish connection to Aerospike server

            if (client == null || !client.isConnected()) {
                System.out.printf("\nERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!");

            } else {
                System.out.printf("\nINFO: Connection to Aerospike cluster succeeded!\n");

                // Create instance of UserService
                // UserService us = new UserService(client);
                // Create instance of TweetService
                // TweetService ts = new TweetService(client);
                // Create instance of UtilityService
                // UtilityService util = new UtilityService(client);

                // Present options
                /*
                System.out.printf("\nWhat would you like to do:\n");
                System.out.printf("1> Create A User And A Tweet\n");
                System.out.printf("2> Read A User Record\n");
                System.out.printf("3> Batch Read Tweets For A User\n");
                System.out.printf("4> Scan All Tweets For All Users\n");
                System.out.printf("5> Record UDF -- Update User Password\n");
                System.out.printf("6> Query Tweets By Username And Users By Tweet Count Range\n");
                System.out.printf("7> Stream UDF -- Aggregation Based on Tweet Count By Region\n");
                System.out.printf("0> Exit\n");
                System.out.printf("\nSelect 0-7 and hit enter:\n");
                */
                int feature = 25;

                TransactionService service = null;

                if (feature != 0) {
                    switch (feature) {
                        case 1:
                            System.out.printf("\n********** Your Selection: Create Transaction **********\n");
                            // us.createUser();
                            // ts.createTweet();
                            service = new TransactionService(client);
                            service.createTransaction();
                            System.out.printf("\n********** Your Selection: Create Transaction Complete **********\n");
                            //break;
                        case 2:
                            System.out.printf("\n********** Your Selection: Read A User Record **********\n");
                            // us.getUser();
                            service = new TransactionService(client);
                            service.readTransaction(1);
                           // break;

                        case 23:
                            service = new TransactionService(client);
                            service.readTransaction(1);
                            service.updateTransaction(1);
                           // break;

                                               case 24:
                            System.out.printf("\n********** Delete Transaction **********\n");
                            // ts.createTweets();

                            service = new TransactionService(client);
                            service.readTransaction(1);

                            service.deleteTransaction(1);
                            service.readTransaction(1);

                            service.sumWinningsTransaction();

                            break;

                        case 25:
                            System.out.printf("\n********** Sum Transaction **********\n");

                            service = new TransactionService(client);
                            service.create100Transactions();
                            service.sumWinningsTransaction();

                            break;


                        default:
                            System.out.printf("\n********** ERROR Feature not found " + feature + " **********\n");
                            break;
                    }
                }
            }
        } catch (AerospikeException e) {
            System.out.printf("AerospikeException - Message: " + e.getMessage()
                    + "\n");
           // System.out.printf("AerospikeException - StackTrace: "
             //       + System.err.printStackTrace(e) + "\n");
        } catch (Exception e) {
            System.out.printf("Exception - Message: " + e.getMessage() + "\n");
           // System.out.printf("Exception - StackTrace: "
            //        + UtilityService.printStackTrace(e) + "\n");
        } finally {
            if (client != null && client.isConnected()) {
                // Close Aerospike server connection
                client.close();
            }
            System.out.printf("\n\nINFO: Press any key to exit...\n");

        }
    }

}