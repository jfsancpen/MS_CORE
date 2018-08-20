package es.um.core.datasource;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import oracle.ucp.UniversalConnectionPoolAdapter;
import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

@Configuration
@ConfigurationProperties(prefix = "datasource")
@ConditionalOnProperty(name = { "datasource.enable" }, havingValue = "true")
public class OracleDataSourceGuard {

	protected static final Logger LOGGER = LoggerFactory.getLogger(OracleDataSourceGuard.class);

	@Value("${datasource.enable:false}")
	private Boolean enable;
	@Value("${datasource.cacheName:cache}")
	private String connectionPoolName;
	@Value("${datasource.connectionURL:}")
	private String connectionURL;
	@Value("${datasource.userName:}")
	private String userName;
	@Value("${datasource.userPassword:}")
	private String userPassword;
	@Value("${datasource.schema:}")
	private String schema;
	@Value("${datasource.minLimit:0}")
	private String minLimit;
	@Value("${datasource.initialLimit:1}")
	private String initialLimit;
	@Value("${datasource.properties.maxLimit:100}")
	private String maxLimit;
	@Value("${datasource.properties.inactivityConnectionTimeout:300}")
	private String inactivityConnectionTimeout;
	@Value("${datasource.properties.timeToLiveTimeout:30}")
	private String timeToLiveTimeout;
	@Value("${datasource.properties.connectionWaitTimeout:9}")
	private String connectionWaitTimeout;
	@Value("${datasource.properties.abandonedConnectionTimeout:30}")
	private String abandonedConnectionTimeout;
	@Value("${datasource.properties.propertyCheckInterval:150}")
	private String propertyCheckInterval;
	@Value("${datasource.configONS:}")
	private String configONS;

	@Bean
	public DataSource datasource() {

		// Elemento DataSource a retornar
		DataSource dataSource = null;

		// Creamos el pool universal de conexiones
		try {
			LOGGER.info("  --> Cargando Driver JDBC...");
			UniversalConnectionPoolManager universalConnectionPoolManager = UniversalConnectionPoolManagerImpl
					.getUniversalConnectionPoolManager();
			// Obtenemos un datasource del pool
			PoolDataSource poolDataSource = PoolDataSourceFactory.getPoolDataSource();

			if (this.enable.booleanValue()) {
				// Establecemos las propiedades de la base de datos
				Properties connectionProperties = new Properties();
				connectionProperties.setProperty("MinLimit", this.minLimit);
				connectionProperties.setProperty("InitialLimit", this.initialLimit);
				connectionProperties.setProperty("MaxLimit", this.maxLimit);
				connectionProperties.setProperty("ValidateConnection", "TRUE");
				connectionProperties.setProperty("InactivityConnectionTimeout", this.inactivityConnectionTimeout);
				connectionProperties.setProperty("TimeToLiveTimeout", this.timeToLiveTimeout);
				connectionProperties.setProperty("ConnectionWaitTimeout", this.connectionWaitTimeout);
				connectionProperties.setProperty("AbandonedConnectionTimeout", this.abandonedConnectionTimeout);
				connectionProperties.setProperty("PropertyCheckInterval", this.propertyCheckInterval);
				
				// Establecemos las propiedades del datasource asi como el usuario y contraseña
				poolDataSource.setURL(this.connectionURL);
				poolDataSource.setUser(this.userName);
				poolDataSource.setPassword(this.userPassword);
				poolDataSource.setConnectionProperties(connectionProperties);
				poolDataSource.setFastConnectionFailoverEnabled(true);
				if (StringUtils.hasText(this.configONS)) {
					poolDataSource.setONSConfiguration(this.configONS);
				}
				poolDataSource.setConnectionPoolName(this.connectionPoolName);
				if (StringUtils.hasText(this.schema)) {
					PreparedStatement ps = poolDataSource.getConnection()
							.prepareStatement("ALTER SESSION SET CURRENT_SCHEMA=" + this.schema);
					ps.executeQuery();
				}
				
				universalConnectionPoolManager.createConnectionPool((UniversalConnectionPoolAdapter)poolDataSource);
				LOGGER.info("  --> Driver JDBC cargado con exito.");
				dataSource = poolDataSource;
			}

		} catch (UniversalConnectionPoolException | SQLException e) {
			LOGGER.error("ERROR: Cargando Driver JDBC", e);
			dataSource = null;
		}

		return dataSource;
	}
}
