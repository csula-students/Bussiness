package edu.csula.datascience.acquisition;

import java.io.IOException;
import java.util.Collection;

import twitter4j.Status;

public class CollectData {

	public static void main(String[] args) {
		offlineCollector offlineC = new offlineCollector();
		offlineC.getData();
		
		onlineCollector onlineC = new onlineCollector();
		onlineC.getData();
	}

}
