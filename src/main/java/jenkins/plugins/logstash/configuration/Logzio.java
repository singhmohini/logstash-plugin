package jenkins.plugins.logstash.configuration;

import hudson.Extension;
import hudson.util.FormValidation;
import hudson.util.Secret;

import jenkins.plugins.logstash.persistence.LogzioDao;
import jenkins.plugins.logstash.Messages;

import org.apache.commons.lang.StringUtils;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.DataBoundSetter;
import org.kohsuke.stapler.QueryParameter;

import javax.annotation.Nonnull;

public class Logzio extends LogstashIndexer<LogzioDao>
{
    private Secret key;
    private String host;

    @DataBoundConstructor
    public Logzio(){}

    public String getHost(){ return this.host; }

    @DataBoundSetter
    public void setHost(String host){ this.host = host; }

    public String getKey()
    {
        return Secret.toString(key);
    }

    @DataBoundSetter
    public void setKey(String key)
    {
        this.key = Secret.fromString(key);
    }


    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
            return false;
        if (this == obj)
            return true;
        if (getClass() != obj.getClass())
            return false;
        Logzio other = (Logzio) obj;
        if (!Secret.toString(key).equals(other.getKey()))
        {
            return false;
        }
        if (host == null)
        {
            return other.host == null;
        }
        else return host.equals(other.host);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + Secret.toString(key).hashCode();
        return result;
    }

    @Override
    public LogzioDao createIndexerInstance() { return new LogzioDao(host, Secret.toString(key)); }

    @Extension
    public static class LogzioDescriptor extends LogstashIndexerDescriptor
    {
        @Nonnull
        @Override
        public String getDisplayName()
        {
            return "Logz.io";
        }

        @Override
        public int getDefaultPort()
        {
            return 0;
        }

        public FormValidation doCheckKey(@QueryParameter("value") String value)
        {
            if (StringUtils.isBlank(value))
            {
                return FormValidation.error(Messages.ValueIsRequired());
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckHost(@QueryParameter("value") String value)
        {
            if (StringUtils.isBlank(value))
            {
                return FormValidation.error(Messages.ValueIsRequired());
            }
            return FormValidation.ok();
        }
    }
}
