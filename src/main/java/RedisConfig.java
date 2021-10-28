class RedisConfig {

    private String host;
    private int port;

    RedisConfig(String host, int port) {
        this.host = host;
        this.port = port;
    }

    //for jackson
    public RedisConfig() {
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
