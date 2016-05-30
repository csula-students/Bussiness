package edu.csula.datascience.acquisition;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * A mock implementation of collector for testing
 */
public class MockCollector implements Collector<SimpleModel, MockData> {
    public Collection<SimpleModel> mungee(Collection<MockData> src) {
        // in your example, you might need to check src.hasNext() first
        return src
            .stream()
            .filter(data -> data.getLocation() != "")
            .map(SimpleModel::build)
            .collect(Collectors.toList());
    }

	@Override
	public Collection<SimpleModel> download(Source<MockData> src) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean save(Collection<SimpleModel> data) {
		// TODO Auto-generated method stub
		return false;
	}
}
