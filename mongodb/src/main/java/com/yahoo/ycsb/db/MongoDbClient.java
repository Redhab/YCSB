/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import com.mongodb.*;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB client for YCSB framework.
 * <p/>
 * Properties to set:
 * <p/>
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=normal
 *
 * @author ypai
 */
public class MongoDbClient extends DB {

    /**
     * Used to include a field in a response.
     */
    protected static final Integer INCLUDE = 1;

    /**
     * A singleton Mongo instance.
     */
    private static MongoClient mongo;

    /**
     * The default write concern for the test.
     */
    private static WriteConcern writeConcern;

    /**
     * The database to access.
     */
    private static String database;

    /**
     * The document key name.
     */
    private static String keyname;

    /**
     * Count the number of times initialized to teardown on the last {@link #cleanup()}.
     */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongo != null) {
                return;
            }

            // initialize MongoDb driver
            Properties props = getProperties();
            String url = props.getProperty("mongodb.url",
                    "mongodb://localhost:27017");
            database = props.getProperty("mongodb.database", "ycsb");
            keyname = props.getProperty("mongodb.keyname", "_id");
            String writeConcernType = props.getProperty("mongodb.writeConcern",
                    "acknowledged").toLowerCase();
            final String maxConnections = props.getProperty(
                    "mongodb.maxconnections", "10");

//            if ("none".equals(writeConcernType)) {
//                writeConcern = WriteConcern.NONE;
//            }
            if ("acknowledged".equalsIgnoreCase(writeConcernType)) {
                writeConcern = WriteConcern.ACKNOWLEDGED;
            } else if ("safe".equalsIgnoreCase(writeConcernType)) {
                writeConcern = WriteConcern.SAFE;
            } else if ("normal".equalsIgnoreCase(writeConcernType)) {
                writeConcern = WriteConcern.NORMAL;
            } else if ("fsync_safe".equalsIgnoreCase(writeConcernType)) {
                writeConcern = WriteConcern.FSYNC_SAFE;
            } else if ("replicas_safe".equalsIgnoreCase(writeConcernType)) {
                writeConcern = WriteConcern.REPLICAS_SAFE;
            } else {
                System.err
                        .println("ERROR: Invalid writeConcern: '"
                                + writeConcernType
                                + "'. "
                                + "Must be [ acknowledged | safe | normal | fsync_safe | replicas_safe ]");
                System.exit(1);
            }

            try {
                // If URI provided use it as is
                if (url.startsWith("mongodb://")) {
                    mongo = new MongoClient(new MongoClientURI(url));
                    System.out.println("database url = " + url);
                } else {
                    String[] params = url.split(":");
                    if (params.length < 2) {
                        System.err
                                .println("ERROR: Invalid : mongodb.url: "
                                        + url
                                        + "'. "
                                        + "Must be <host>:<port> format");
                        System.exit(1);
                    }

                    MongoClientOptions.Builder options = new MongoClientOptions.Builder();
                    options.connectionsPerHost(Integer.parseInt(maxConnections));
                    mongo = new MongoClient(
                            new ServerAddress(
                                    params[0], // Host
                                    Integer.parseInt(params[1]) // Port
                            ),
                            options.build()
                    );
                    //System.out.println("new database url = " + url);
                }

                // need to append db to url.
//                url += "/" + database;
//                System.out.println("new database url = " + url);
//                MongoOptions options = new MongoOptions();
//                options.connectionsPerHost = Integer.parseInt(maxConnections);
//                mongo = new Mongo(new DBAddress(url), options);
//                mongo = new MongoClient(new DBAddress(url), options);

                System.out.println("mongo connection created with " + url);
            } catch (Exception e1) {
                System.err
                        .println("Could not initialize MongoDB connection pool for Loader: "
                                + e1.toString());
                e1.printStackTrace();
            }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            try {
                mongo.close();
            } catch (Exception e1) {
                System.err.println("Could not close MongoDB connection pool: "
                        + e1.toString());
                e1.printStackTrace();
            }
        }
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q, writeConcern);
            return res.getN() == 1 ? 0 : 1;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
                      HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject r = new BasicDBObject().append(keyname, key);
            for (String k : values.keySet()) {
                r.put(k, values.get(k).toArray());
            }
            WriteResult res = collection.insert(r, writeConcern);
            return res.getError() == null ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append(keyname, key);
            DBObject fieldsToReturn = new BasicDBObject();

            DBObject queryResult;
            if (fields != null) {
                for (String field : fields) {
                    fieldsToReturn.put(field, INCLUDE);
                }
                queryResult = collection.findOne(q, fieldsToReturn);
            } else {
                queryResult = collection.findOne(q);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
            }
            return queryResult != null ? 0 : 1;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key,
                      HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append(keyname, key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            for (String tmpKey : values.keySet()) {
                fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());

            }
            u.put("$set", fieldsToSet);
            WriteResult res = collection.update(q, u, false, false,
                    writeConcern);
            return res.getN() == 1 ? 0 : 1;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table       The name of the table
     * @param startkey    The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields      The list of fields to read, or null for all of them
     * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            // { keyname:{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append(keyname, scanRange);
            DBCursor cursor = collection.find(q).limit(recordcount);
            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

                DBObject obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            return 0;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }

    }

    /**
     * TODO - Finish
     *
     * @param resultMap
     * @param obj
     */
    @SuppressWarnings("unchecked")
    protected void fillMap(HashMap<String, ByteIterator> resultMap, DBObject obj) {
        Map<String, Object> objMap = obj.toMap();
        for (Map.Entry<String, Object> entry : objMap.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        (byte[]) entry.getValue()));
            }
        }
    }
}
