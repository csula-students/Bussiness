package edu.csula.datascience.acquisition;

import java.util.Collection;
import java.util.List;

/**
 * Interface to define collector behavior
 *
 * It should be able to download data from source and save data.
 */
public interface Collector<T, N> {
    Collection<T> download(Source<N> src);

    boolean save(Collection<T> data);
}
