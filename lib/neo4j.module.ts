import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { Sha256 } from '@aws-crypto/sha256-js';
import { HttpRequest } from '@smithy/protocol-http';
import { SignatureV4 } from '@smithy/signature-v4';
import { DynamicModule, Module, Provider } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import neo4j, { Driver } from 'neo4j-driver';
import { Neo4jConfig, NeptuneConfig } from './interface';
import { NEO4J_CONFIG, NEO4J_DRIVER } from './constant';
import { Neo4jService } from './service';

export const createDriver = async (config: Neo4jConfig) => {
  const { scheme, host, port, username, password, ...driverConfig } = config;

  const auth =
    !username || !password ? undefined : neo4j.auth.basic(username, password);

  const driver: Driver = neo4j.driver(
    `${scheme}://${host}:${port}`,
    auth,
    driverConfig,
  );

  await driver.verifyConnectivity();

  return driver;
};

const signedHeader = async (
  host: string,
  port: number | string,
  region: string,
) => {
  const req = new HttpRequest({
    method: 'GET',
    protocol: 'bolt',
    hostname: host,
    port: typeof port === 'string' ? parseInt(port, 10) : port,
    // Comment out the following line if you're using an engine version older than 1.2.0.0
    path: '/opencypher',
    headers: {
      host: `${host}:${port}`,
    },
  });

  const signer = new SignatureV4({
    credentials: defaultProvider(),
    region,
    service: 'neptune-db',
    sha256: Sha256,
  });

  const signedRequest = await signer.sign(req, {
    unsignableHeaders: new Set(['x-amz-content-sha256']),
  });

  const authInfo = {
    Authorization: signedRequest.headers['authorization'],
    HttpMethod: signedRequest.method,
    'X-Amz-Date': signedRequest.headers['x-amz-date'],
    Host: signedRequest.headers['host'],
    'X-Amz-Security-Token': signedRequest.headers['x-amz-security-token'],
  };

  return JSON.stringify(authInfo);
};

export const createNeptuneDriver = async (config: NeptuneConfig) => {
  const { host, port, region, ...driverConfig } = config;

  const driver: Driver = neo4j.driver(
    `bolt://${host}:${port}`,
    {
      credentials: await signedHeader(host, port, region),
      scheme: 'basic',
      realm: 'realm',
      principal: 'username',
    },
    {
      ...driverConfig,
      encrypted: 'ENCRYPTION_ON',
      trust: 'TRUST_SYSTEM_CA_SIGNED_CERTIFICATES',
    },
  );

  await driver.verifyConnectivity();

  return driver;
};

const isNeo4jConfig = (
  config: NeptuneConfig | Neo4jConfig,
): config is Neo4jConfig => !!(config as Neo4jConfig).username;

@Module({})
export class Neo4jModule {
  static forRoot(config: Neo4jConfig | NeptuneConfig): DynamicModule {
    return {
      module: Neo4jModule,
      global: config.global,
      providers: [
        {
          provide: NEO4J_CONFIG,
          useValue: isNeo4jConfig(config)
            ? config
            : {
                ...config,
                scheme: 'bolt',
              },
        },
        {
          provide: NEO4J_DRIVER,
          inject: [NEO4J_CONFIG],
          useFactory: async (config: Neo4jConfig | NeptuneConfig) =>
            isNeo4jConfig(config)
              ? createDriver(config)
              : createNeptuneDriver(config),
        },
        Neo4jService,
      ],
      exports: [Neo4jService],
    };
  }

  static forRootAsync(configProvider): DynamicModule {
    return {
      module: Neo4jModule,
      global: configProvider.global,
      imports: [ConfigModule],

      providers: [
        {
          provide: NEO4J_CONFIG,
          ...configProvider,
        } as Provider,
        {
          provide: NEO4J_DRIVER,
          inject: [NEO4J_CONFIG],
          useFactory: async (config: Neo4jConfig | NeptuneConfig) =>
            isNeo4jConfig(config)
              ? createDriver(config)
              : createNeptuneDriver(config),
        },
        Neo4jService,
      ],
      exports: [Neo4jService],
    };
  }
}
