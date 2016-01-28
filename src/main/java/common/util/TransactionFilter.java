package common.util;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.log4j.Logger;

import com.gaoshin.dbshard2.RequestContext;


public class TransactionFilter implements Filter {
    private static Logger logger = Logger.getLogger(TransactionFilter.class);
    
	@Override
	public void destroy() {
	}

	@Override
	public void doFilter(ServletRequest req, ServletResponse resp,
			FilterChain chain) throws IOException, ServletException {
		RequestContext rc = RequestContext.getRequestContext();
		
		try {
			chain.doFilter(req, resp);
			rc.commit();
		} catch (Throwable t) {
			try {
				rc.rollback();
			} catch (SQLException e) {
				System.err.println(e.getMessage());
			}
			throw new ServletException(t);
		} 
		finally {
			try {
				rc.close();
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
	        RequestContext.setRequestContext(null);
		}
	}

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }
}
