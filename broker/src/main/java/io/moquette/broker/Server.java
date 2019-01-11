/*
 * Copyright (c) 2012-2018 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.moquette.broker;

import io.moquette.BrokerConstants;
import io.moquette.broker.config.*;
import io.moquette.interception.InterceptHandler;
import io.moquette.persistence.H2Builder;
import io.moquette.persistence.MemorySubscriptionsRepository;
import io.moquette.interception.BrokerInterceptor;
import io.moquette.broker.security.*;
import io.moquette.broker.subscriptions.CTrieSubscriptionDirectory;
import io.moquette.broker.subscriptions.ISubscriptionsDirectory;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.broker.security.IAuthorizatorPolicy;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.moquette.logging.LoggingUtils.getInterceptorIds;

public class Server {

    private static final Logger LOG = LoggerFactory.getLogger(io.moquette.broker.Server.class);

    private NewNettyAcceptor acceptor;
    private volatile boolean initialized;
    private PostOffice dispatcher;
    private BrokerInterceptor interceptor;

    public static void main(String[] args) throws IOException {
        final Server server = new Server();
        server.startServer();
        System.out.println("Server started, version 0.12.1-SNAPSHOT");
        //Bind a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(server::stopServer));
    }

    /**
     * Starts Moquette bringing the configuration from the file located at m_config/moquette.conf
     *
     * @throws IOException in case of any IO error.
     */
    public void startServer() throws IOException {
        File defaultConfigurationFile = defaultConfigFile();
        LOG.info("Starting Moquette integration. Configuration file path={}", defaultConfigurationFile.getAbsolutePath());
        IResourceLoader filesystemLoader = new FileResourceLoader(defaultConfigurationFile);
        final IConfig config = new ResourceLoaderConfig(filesystemLoader);
        startServer(config, null, null, null, null);
    }

    private static File defaultConfigFile() {
        String configPath = System.getProperty("moquette.path", null);
        return new File(configPath, IConfig.DEFAULT_CONFIG);
    }

    public void startServer(IConfig config, List<? extends InterceptHandler> handlers, ISslContextCreator sslCtxCreator,
                            IAuthenticator authenticator, IAuthorizatorPolicy authorizatorPolicy) throws IOException {
        final long start = System.currentTimeMillis();
        if (handlers == null) {
            handlers = Collections.emptyList();
        }
        LOG.trace("Starting Moquette Server. MQTT message interceptors={}", getInterceptorIds(handlers));

        final String handlerProp = System.getProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME);
        if (handlerProp != null) {
            config.setProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME, handlerProp);
        }
        final String persistencePath = config.getProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME);
        LOG.debug("Configuring Using persistent store file, path: {}", persistencePath);
        initInterceptors(config, handlers);
        LOG.debug("Initialized MQTT protocol processor");
        if (sslCtxCreator == null) {
            LOG.info("Using default SSL context creator");
            sslCtxCreator = new DefaultMoquetteSslContextCreator(config);
        }
        authenticator = initializeAuthenticator(authenticator, config);
        authorizatorPolicy = initializeAuthorizatorPolicy(authorizatorPolicy, config);

        final ISubscriptionsRepository subscriptionsRepository;
        final IQueueRepository queueRepository;
        final IRetainedRepository retainedRepository;

        LOG.trace("Configuring in-memory subscriptions store");
        subscriptionsRepository = new MemorySubscriptionsRepository();
        queueRepository = new MemoryQueueRepository();
        retainedRepository = new MemoryRetainedRepository();

        ISubscriptionsDirectory subscriptions = new CTrieSubscriptionDirectory();
        subscriptions.init(subscriptionsRepository);
        SessionRegistry sessions = new SessionRegistry(subscriptions, queueRepository);
        dispatcher = new PostOffice(subscriptions, authorizatorPolicy, retainedRepository, sessions, interceptor);
        final BrokerConfiguration brokerConfig = new BrokerConfiguration(config);
        MQTTConnectionFactory connectionFactory = new MQTTConnectionFactory(brokerConfig, authenticator, sessions,
                                                                            dispatcher);

        final NewNettyMQTTHandler mqttHandler = new NewNettyMQTTHandler(connectionFactory);
        acceptor = new NewNettyAcceptor();
        acceptor.initialize(mqttHandler, config, sslCtxCreator);

        final long startTime = System.currentTimeMillis() - start;
        LOG.info("Moquette integration has been started successfully in {} ms", startTime);
        initialized = true;
    }

    private IAuthorizatorPolicy initializeAuthorizatorPolicy(IAuthorizatorPolicy authorizatorPolicy, IConfig props) {
//        if (authorizatorPolicy == null) {
//            authorizatorPolicy = new PermitAllAuthorizatorPolicy();
//        }
//        return authorizatorPolicy;

        LOG.debug("Configuring MQTT authorizator policy");
        String authorizatorClassName = props.getProperty(BrokerConstants.AUTHORIZATOR_CLASS_NAME, "");
        if (authorizatorPolicy == null && !authorizatorClassName.isEmpty()) {
            authorizatorPolicy = loadClass(authorizatorClassName, IAuthorizatorPolicy.class, IConfig.class, props);
        }

        if (authorizatorPolicy == null) {
            String aclFilePath = props.getProperty(BrokerConstants.ACL_FILE_PROPERTY_NAME, "");
            if (aclFilePath != null && !aclFilePath.isEmpty()) {
                authorizatorPolicy = new DenyAllAuthorizatorPolicy();
                try {
                    LOG.info("Parsing ACL file. Path = {}", aclFilePath);
                    IResourceLoader resourceLoader = props.getResourceLoader();
                    authorizatorPolicy = ACLFileParser.parse(resourceLoader.loadResource(aclFilePath));
                } catch (ParseException pex) {
                    LOG.error("Unable to parse ACL file. path=" + aclFilePath, pex);
                }
            } else {
                authorizatorPolicy = new PermitAllAuthorizatorPolicy();
            }
            LOG.info("Authorizator policy {} instance will be used", authorizatorPolicy.getClass().getName());
        }
        return authorizatorPolicy;
    }

    private IAuthenticator initializeAuthenticator(IAuthenticator authenticator, IConfig props) {
//        if (authenticator == null) {
//            authenticator = new AcceptAllAuthenticator();
//        }
//        return authenticator;

        LOG.debug("Configuring MQTT authenticator");
        String authenticatorClassName = props.getProperty(BrokerConstants.AUTHENTICATOR_CLASS_NAME, "");

        if (authenticator == null && !authenticatorClassName.isEmpty()) {
            authenticator = loadClass(authenticatorClassName, IAuthenticator.class, IConfig.class, props);
        }

        IResourceLoader resourceLoader = props.getResourceLoader();
        if (authenticator == null) {
            String passwdPath = props.getProperty(BrokerConstants.PASSWORD_FILE_PROPERTY_NAME, "");
            if (passwdPath.isEmpty()) {
                authenticator = new AcceptAllAuthenticator();
            } else {
                authenticator = new ResourceAuthenticator(resourceLoader, passwdPath);
            }
            LOG.info("An {} authenticator instance will be used", authenticator.getClass().getName());
        }
        return authenticator;
    }

    private void initInterceptors(IConfig props, List<? extends InterceptHandler> embeddedObservers) {
        LOG.info("Configuring message interceptors...");

        List<InterceptHandler> observers = new ArrayList<>(embeddedObservers);
        String interceptorClassName = props.getProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME);
        if (interceptorClassName != null && !interceptorClassName.isEmpty()) {
            InterceptHandler handler = loadClass(interceptorClassName, InterceptHandler.class,
                                                 io.moquette.broker.Server.class, this);
            if (handler != null) {
                observers.add(handler);
            }
        }
        interceptor = new BrokerInterceptor(props, observers);
    }

    @SuppressWarnings("unchecked")
    private <T, U> T loadClass(String className, Class<T> intrface, Class<U> constructorArgClass, U props) {
        T instance = null;
        try {
            // check if constructor with constructor arg class parameter
            // exists
            LOG.info("Invoking constructor with {} argument. ClassName={}, interfaceName={}",
                     constructorArgClass.getName(), className, intrface.getName());
            instance = this.getClass().getClassLoader()
                .loadClass(className)
                .asSubclass(intrface)
                .getConstructor(constructorArgClass)
                .newInstance(props);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
            LOG.warn("Unable to invoke constructor with {} argument. ClassName={}, interfaceName={}, cause={}, " +
                     "errorMessage={}", constructorArgClass.getName(), className, intrface.getName(), ex.getCause(),
                     ex.getMessage());
            return null;
        } catch (NoSuchMethodException | InvocationTargetException e) {
            try {
                LOG.info("Invoking default constructor. ClassName={}, interfaceName={}", className, intrface.getName());
                // fallback to default constructor
                instance = this.getClass().getClassLoader()
                    .loadClass(className)
                    .asSubclass(intrface)
                    .getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException |
                NoSuchMethodException | InvocationTargetException ex) {
                LOG.error("Unable to invoke default constructor. ClassName={}, interfaceName={}, cause={}, " +
                          "errorMessage={}", className, intrface.getName(), ex.getCause(), ex.getMessage());
                return null;
            }
        }

        return instance;
    }

    /**
     * Use the broker to publish a message. It's intended for embedding applications. It can be used
     * only after the integration is correctly started with startServer.
     *
     * @param msg      the message to forward.
     * @param clientId the id of the sending integration.
     * @throws IllegalStateException if the integration is not yet started
     */
    public void internalPublish(MqttPublishMessage msg, final String clientId) {
        final int messageID = msg.variableHeader().packetId();
        if (!initialized) {
            LOG.error("Moquette is not started, internal message cannot be published. CId: {}, messageId: {}", clientId,
                      messageID);
            throw new IllegalStateException("Can't publish on a integration is not yet started");
        }
        LOG.trace("Internal publishing message CId: {}, messageId: {}", clientId, messageID);
        dispatcher.internalPublish(msg);
    }

    public void stopServer() {
        LOG.info("Unbinding integration from the configured ports");
        acceptor.close();
        LOG.trace("Stopping MQTT protocol processor");
        initialized = false;

        LOG.info("Moquette integration has been stopped.");
    }

    /**
     * SPI method used by Broker embedded applications to get list of subscribers. Returns null if
     * the broker is not started.
     *
     * @return list of subscriptions.
     */
// TODO reimplement this
//    public List<Subscription> getSubscriptions() {
//        if (m_processorBootstrapper == null) {
//            return null;
//        }
//        return this.subscriptionsStore.listAllSubscriptions();
//    }

    /**
     * SPI method used by Broker embedded applications to add intercept handlers.
     *
     * @param interceptHandler the handler to add.
     */
    public void addInterceptHandler(InterceptHandler interceptHandler) {
        if (!initialized) {
            LOG.error("Moquette is not started, MQTT message interceptor cannot be added. InterceptorId={}",
                interceptHandler.getID());
            throw new IllegalStateException("Can't register interceptors on a integration that is not yet started");
        }
        LOG.info("Adding MQTT message interceptor. InterceptorId={}", interceptHandler.getID());
        interceptor.addInterceptHandler(interceptHandler);
    }

    /**
     * SPI method used by Broker embedded applications to remove intercept handlers.
     *
     * @param interceptHandler the handler to remove.
     */
    public void removeInterceptHandler(InterceptHandler interceptHandler) {
        if (!initialized) {
            LOG.error("Moquette is not started, MQTT message interceptor cannot be removed. InterceptorId={}",
                interceptHandler.getID());
            throw new IllegalStateException("Can't deregister interceptors from a integration that is not yet started");
        }
        LOG.info("Removing MQTT message interceptor. InterceptorId={}", interceptHandler.getID());
        interceptor.removeInterceptHandler(interceptHandler);
    }
}
