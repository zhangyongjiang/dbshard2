package common.util;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;

import com.bsci.dbshard2.RequestContext;


public class TransactionFilter implements Filter {
    private static Logger logger = Logger.getLogger(TransactionFilter.class);
    
	@Override
	public void destroy() {
	}
	
	private String getRemoteIp(HttpServletRequest req) {
		String ip = req.getHeader("x-forwarded-for");
		if(ip == null)
			ip = req.getRemoteAddr();
		return ip;
	}

	@Override
	public void doFilter(ServletRequest req, ServletResponse resp,
			FilterChain chain) throws IOException, ServletException {
		RequestContext rc = RequestContext.getRequestContext();
		rc.remoteIp = getRemoteIp((HttpServletRequest) req);
		rc.reqId = ((HttpServletRequest) req).getHeader("reqId");
		
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
			rc.endTime = System.currentTimeMillis();
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
