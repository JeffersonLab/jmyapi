package org.jlab.mya;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.spi.InitialContextFactory;

/**
 * Support class for testing JNDI.
 * 
 * @author slominskir
 */
public class StandaloneJndi implements InitialContextFactory {
    
    private static final MemoryContext INIT_CTX = new MemoryContext();
    
    static {
        try {
            INIT_CTX.rebind("java:comp/env", new MemoryContext());
        } catch (Exception ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }
    
    public StandaloneJndi() {
                System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                "org.jlab.mya.StandaloneJndi");
    }

    @Override
    public Context getInitialContext(
            Hashtable<?, ?> environment) {
        return INIT_CTX;
    }
    
    private static class MemoryContext implements Context {
        
        private static final Map<String, Object> DB = new HashMap<>();
        
        @Override
        public Object lookup(Name name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Object lookup(String name) {
            return DB.get(name);
        }

        @Override
        public void bind(Name name, Object obj) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void bind(String name, Object obj) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void rebind(Name name, Object obj) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void rebind(String name, Object obj) {
            DB.put(name, obj);
        }

        @Override
        public void unbind(Name name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void unbind(String name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void rename(Name oldName, Name newName) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void rename(String oldName, String newName) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public NamingEnumeration<NameClassPair> list(Name name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public NamingEnumeration<NameClassPair> list(String name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public NamingEnumeration<Binding> listBindings(Name name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public NamingEnumeration<Binding> listBindings(String name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void destroySubcontext(Name name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void destroySubcontext(String name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Context createSubcontext(Name name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Context createSubcontext(String name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Object lookupLink(Name name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Object lookupLink(String name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public NameParser getNameParser(Name name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public NameParser getNameParser(String name) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Name composeName(Name name, Name prefix) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String composeName(String name, String prefix) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Object addToEnvironment(String propName, Object propVal) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Object removeFromEnvironment(String propName) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Hashtable<?, ?> getEnvironment() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String getNameInNamespace() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    
}
}
