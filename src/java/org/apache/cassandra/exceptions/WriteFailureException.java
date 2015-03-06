package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

public class WriteFailureException extends RequestFailureException {

    public final WriteType writeType;

	public WriteFailureException(ConsistencyLevel consistency, int received, int failures, int blockFor, WriteType writeType)
    {
        super(ExceptionCode.WRITE_FAILURE, consistency, received, failures, blockFor);
        this.writeType = writeType;
    }
}
