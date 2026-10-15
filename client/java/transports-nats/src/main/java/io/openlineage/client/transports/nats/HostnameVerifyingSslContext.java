/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.SecureRandom;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

/**
 * Wraps an {@link SSLContext} so every client socket and engine it creates checks that the server
 * certificate matches the host it connects to. jnats validates the certificate chain but performs
 * no hostname verification, so without this any certificate from a trusted CA is accepted.
 */
final class HostnameVerifyingSslContext {
  private static final String ENDPOINT_IDENTIFICATION = "HTTPS";

  private HostnameVerifyingSslContext() {}

  static SSLContext wrap(SSLContext delegate) {
    return new SSLContext(new Spi(delegate), delegate.getProvider(), delegate.getProtocol()) {};
  }

  private static <T extends Socket> T verifying(T socket) {
    if (socket instanceof SSLSocket) {
      SSLSocket sslSocket = (SSLSocket) socket;
      SSLParameters parameters = sslSocket.getSSLParameters();
      parameters.setEndpointIdentificationAlgorithm(ENDPOINT_IDENTIFICATION);
      sslSocket.setSSLParameters(parameters);
    }
    return socket;
  }

  private static SSLEngine verifying(SSLEngine engine) {
    SSLParameters parameters = engine.getSSLParameters();
    parameters.setEndpointIdentificationAlgorithm(ENDPOINT_IDENTIFICATION);
    engine.setSSLParameters(parameters);
    return engine;
  }

  private static final class Spi extends SSLContextSpi {
    private final SSLContext delegate;

    Spi(SSLContext delegate) {
      this.delegate = delegate;
    }

    @Override
    protected void engineInit(
        KeyManager[] keyManagers, TrustManager[] trustManagers, SecureRandom random)
        throws KeyManagementException {
      delegate.init(keyManagers, trustManagers, random);
    }

    @Override
    protected SSLSocketFactory engineGetSocketFactory() {
      return new VerifyingSocketFactory(delegate.getSocketFactory());
    }

    @Override
    protected SSLServerSocketFactory engineGetServerSocketFactory() {
      return delegate.getServerSocketFactory();
    }

    @Override
    protected SSLEngine engineCreateSSLEngine() {
      return delegate.createSSLEngine();
    }

    @Override
    protected SSLEngine engineCreateSSLEngine(String host, int port) {
      return verifying(delegate.createSSLEngine(host, port));
    }

    @Override
    protected SSLSessionContext engineGetServerSessionContext() {
      return delegate.getServerSessionContext();
    }

    @Override
    protected SSLSessionContext engineGetClientSessionContext() {
      return delegate.getClientSessionContext();
    }
  }

  private static final class VerifyingSocketFactory extends SSLSocketFactory {
    private final SSLSocketFactory delegate;

    VerifyingSocketFactory(SSLSocketFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return delegate.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket() throws IOException {
      return verifying(delegate.createSocket());
    }

    @Override
    public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
        throws IOException {
      return verifying(delegate.createSocket(socket, host, port, autoClose));
    }

    @Override
    public Socket createSocket(Socket socket, InputStream consumed, boolean autoClose)
        throws IOException {
      return verifying(delegate.createSocket(socket, consumed, autoClose));
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
      return verifying(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
        throws IOException {
      return verifying(delegate.createSocket(host, port, localHost, localPort));
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
      return verifying(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(
        InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
      return verifying(delegate.createSocket(address, port, localAddress, localPort));
    }
  }
}
