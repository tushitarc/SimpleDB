package simpledb.remote;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;

/**
 * An adapter class that wraps RemoteResultSet.
 * Its methods do nothing except transform RemoteExceptions
 * into SQLExceptions.
 * @author Edward Sciore
 */
public class SimpleResultSet extends ResultSetAdapter {
   private RemoteResultSet rrs;
   
   public SimpleResultSet(RemoteResultSet s) {
      rrs = s;
   }
   
   public boolean next() throws SQLException {
      try {
         return rrs.next();
      }
      catch (Exception e) {
         throw new SQLException(e);
      }
   }
   
   public int getInt(String fldname) throws SQLException {
      try {
         return rrs.getInt(fldname);
      }
      catch (Exception e) {
         throw new SQLException(e);
      }
   }
   
   public String getString(String fldname) throws SQLException {
      try {
         return rrs.getString(fldname);
      }
      catch (Exception e) {
         throw new SQLException(e);
      }
   }
   
   public ResultSetMetaData getMetaData() throws SQLException {
      try {
         RemoteMetaData rmd = rrs.getMetaData();
         return new SimpleMetaData(rmd);
      }
      catch (Exception e) {
         throw new SQLException(e);
      }
   }
   
   public void close() throws SQLException {
      try {
         rrs.close();
      }
      catch (Exception e) {
         throw new SQLException(e);
      }
   }

@Override
public long getLong(int columnIndex) throws SQLException {
	// TODO Auto-generated method stub
	return 0;
}

@Override
public long getLong(String columnLabel) throws SQLException {
	// TODO Auto-generated method stub
	return 0;
}

@Override
public void updateLong(int columnIndex, long x) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateLong(String columnLabel, long x) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateNCharacterStream(int columnIndex, Reader x, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateNCharacterStream(String columnLabel, Reader reader,
		long length) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateAsciiStream(int columnIndex, InputStream x, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateBinaryStream(int columnIndex, InputStream x, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateCharacterStream(int columnIndex, Reader x, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateAsciiStream(String columnLabel, InputStream x, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateBinaryStream(String columnLabel, InputStream x, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateCharacterStream(String columnLabel, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateBlob(int columnIndex, InputStream inputStream, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateBlob(String columnLabel, InputStream inputStream, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateClob(int columnIndex, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateClob(String columnLabel, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateNClob(int columnIndex, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void updateNClob(String columnLabel, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}
}

