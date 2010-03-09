package test.casserver;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.inspektr.common.ioc.annotation.NotEmpty;
import org.inspektr.common.ioc.annotation.NotNull;
import org.jasig.cas.authentication.principal.Service;
import org.jasig.cas.web.support.ArgumentExtractor;
import org.jasig.cas.web.support.CookieRetrievingCookieGenerator;
import org.jasig.cas.web.support.WebUtils;
import org.springframework.util.StringUtils;
import org.springframework.webflow.action.AbstractAction;
import org.springframework.webflow.execution.Event;
import org.springframework.webflow.execution.RequestContext;







/**
 * Զ�̵�½Ʊ���ṩAction.
 * ����InitialFlowSetupAction�޸�.
 * ����InitialFlowSetupActionΪfinal�࣬���ֻ�ܽ����븴�ƹ����ٽ����޸�.
 * 
 * @author GuoLin
 */
public class RemoteLoginAction extends AbstractAction {
	
	static Log log = LogFactory.getLog(RemoteLoginAction.class);
	
    /** CookieGenerator for the Warnings. */
    @NotNull
    private CookieRetrievingCookieGenerator warnCookieGenerator;
    /** CookieGenerator for the TicketGrantingTickets. */
    @NotNull
    private CookieRetrievingCookieGenerator ticketGrantingTicketCookieGenerator;
    /** Extractors for finding the service. */
    @NotEmpty
    private List<ArgumentExtractor> argumentExtractors;
    /** Boolean to note whether we've set the values on the generators or not. */
    private boolean pathPopulated = false;
    protected Event doExecute(final RequestContext context) throws Exception {
        
    	log.debug("enter RemoteLoginAction...");
    	
    	final HttpServletRequest request = WebUtils.getHttpServletRequest(context);
        if (!this.pathPopulated) {
            final String contextPath = context.getExternalContext().getContextPath();
            final String cookiePath = StringUtils.hasText(contextPath) ? contextPath : "/";
            logger.info("Setting path for cookies to: " + cookiePath);
            this.warnCookieGenerator.setCookiePath(cookiePath);
            this.ticketGrantingTicketCookieGenerator.setCookiePath(cookiePath);
            this.pathPopulated = true;
        }
        context.getFlowScope().put("ticketGrantingTicketId", 
                this.ticketGrantingTicketCookieGenerator.retrieveCookieValue(request));
        context.getFlowScope().put("warnCookieValue",
                Boolean.valueOf(this.warnCookieGenerator.retrieveCookieValue(request)));
        final Service service = WebUtils.getService(this.argumentExtractors, context);
        if (service != null && logger.isDebugEnabled()) {
            logger.debug("Placing service in FlowScope: " + service.getId());
        }
        context.getFlowScope().put("service", service);
        
        // �ͻ��˱��봫��loginUrl���������������޷�ȷ����½Ŀ��ҳ��
        if (StringUtils.hasText(request.getParameter("loginUrl"))) {
            context.getFlowScope().put("remoteLoginUrl", request.getParameter("loginUrl"));
        } else {
            request.setAttribute("remoteLoginMessage", "loginUrl parameter must be supported.");
            return error();
        }
        
        // ����������submit������ύ�����������֤
        if (StringUtils.hasText(request.getParameter("submit"))) {
            return result("submit");
        } else {
            return result("checkTicketGrantingTicket");
        }
    }
    public void setTicketGrantingTicketCookieGenerator(
        final CookieRetrievingCookieGenerator ticketGrantingTicketCookieGenerator) {
        this.ticketGrantingTicketCookieGenerator = ticketGrantingTicketCookieGenerator;
    }
    public void setWarnCookieGenerator(final CookieRetrievingCookieGenerator warnCookieGenerator) {
        this.warnCookieGenerator = warnCookieGenerator;
    }
    public void setArgumentExtractors(
        final List<ArgumentExtractor> argumentExtractors) {
        this.argumentExtractors = argumentExtractors;
    }
}


