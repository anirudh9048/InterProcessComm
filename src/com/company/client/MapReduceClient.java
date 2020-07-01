package com.company.client;

/*
* This class will be instantiated by the user code that wants to use map reduce.
* Handles all communications with the compute cluster.
* */

import java.util.List;
import java.util.function.Function;

public class MapReduceClient {
    Function<String, String> mapUserFn; // write (inputStr, mapUserFn.apply(inputStr)) to intermediate file
    Function<List<String>, String> reduceUserFn; // write (accStr,  reduceUserFn.apply(stringList) to output file

    public MapReduceClient(Function<String, String> mapUserFnIn , Function<List<String>, String> reduceUserFnIn) {
        this.mapUserFn = mapUserFnIn;
        this.reduceUserFn = reduceUserFnIn;
    }

    // blocked on finding a way to serialize the map and reduce functions and send them over the network

}
