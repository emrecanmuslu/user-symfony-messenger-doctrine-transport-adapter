<?php

namespace EmrecanMuslu\Messenger\Transport\NotificationTransport;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Driver\Result as DriverResult;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\LockMode;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Synchronizer\SchemaSynchronizer;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Types;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Contracts\Service\ResetInterface;

class Connection implements ResetInterface
{
    protected const TABLE_OPTION_NAME = '_symfony_messenger_table_name';

    protected const DEFAULT_OPTIONS = [
        'table_name' => 'notification_sms',
        'provider_name' => null,
        'template_name' => null,
        'redeliver_timeout' => 3600,
        'receive_timeout' => 60000,
        'auto_setup' => true,
        'payload' => [
            "models" => []
        ]
    ];

    /**
     * Configuration of the connection.
     *
     * Available options:
     *
     * * table_name: name of the table
     * * connection: name of the Doctrine's entity manager
     * * provider_name: name of the queue
     * * redeliver_timeout: Timeout before redeliver messages still in handling state (i.e: delivered_at is not null and message is still in table). Default: 3600
     * * auto_setup: Whether the table should be created automatically during send / get. Default: true
     */
    protected $configuration = [];
    protected $driverConnection;
    protected $queueEmptiedAt;
    private $schemaSynchronizer;
    private $autoSetup;
    private $logger;

    public function __construct(array $configuration, DBALConnection $driverConnection, SchemaSynchronizer $schemaSynchronizer = null, LoggerInterface $logger)
    {
        $this->configuration = array_replace_recursive(static::DEFAULT_OPTIONS, $configuration);
        $this->driverConnection = $driverConnection;
        $this->schemaSynchronizer = $schemaSynchronizer;
        $this->logger = $logger;
        $this->autoSetup = $this->configuration['auto_setup'];
    }

    public function reset()
    {
        $this->queueEmptiedAt = null;
    }

    public function getConfiguration(): array
    {
        return $this->configuration;
    }

    public static function buildConfiguration(string $dsn, array $options = []): array
    {
        if (false === $components = parse_url($dsn)) {
            throw new InvalidArgumentException(sprintf('The given Doctrine Messenger DSN "%s" is invalid.', $dsn));
        }

        $query = [];
        if (isset($components['query'])) {
            parse_str($components['query'], $query);
        }

        $configuration = ['connection' => $components['host']];
        $configuration += $query + $options + static::DEFAULT_OPTIONS;

        $configuration['auto_setup'] = filter_var($configuration['auto_setup'], \FILTER_VALIDATE_BOOLEAN);

        // check for extra keys in options
        $optionsExtraKeys = array_diff(array_keys($options), array_keys(static::DEFAULT_OPTIONS));
        if (0 < \count($optionsExtraKeys)) {
            throw new InvalidArgumentException(sprintf('Unknown option found: [%s]. Allowed options are [%s].', implode(', ', $optionsExtraKeys), implode(', ', array_keys(static::DEFAULT_OPTIONS))));
        }

        // check for extra keys in options
        $queryExtraKeys = array_diff(array_keys($query), array_keys(static::DEFAULT_OPTIONS));
        if (0 < \count($queryExtraKeys)) {
            throw new InvalidArgumentException(sprintf('Unknown option found in DSN: [%s]. Allowed options are [%s].', implode(', ', $queryExtraKeys), implode(', ', array_keys(static::DEFAULT_OPTIONS))));
        }

        return $configuration;
    }

    /**
     * @param int $delay The delay in milliseconds
     *
     * @return string The inserted id
     *
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Exception
     */
    public function send(Envelope $envelope, string $encodedMessage, array $headers, int $delay = 0): string
    {
        $notificationSmsMessage = $envelope->getMessage()->getMessage();
        $notificationSmsTemplate = $notificationSmsMessage->getTemplate();
        $notificationSmsOrderDetail = $notificationSmsMessage->getOrderDetails();
        $orderId = $notificationSmsOrderDetail->getId();
        $template_name = $this->configuration['template_name'];
        $now = new \DateTime();
        $availableAt = (clone $now)->modify(sprintf('+%d seconds', $delay / 1000));

        $checkRowId = $this->createQueryBuilderCheckRow($orderId, $template_name);
        $queryBuilder = $this->driverConnection->createQueryBuilder();

        if(null === $checkRowId || 0 === $checkRowId){
            $queryBuilder->insert($this->configuration['table_name'])
            ->values([
                'order_id' => '?',
                'encoded_message' => '?',
                'headers' => '?',
                'provider_name' => '?',
                'template_name' => '?',
                'handled' => '?',
                'created_at' => '?',
                'available_at' => '?',
            ]);
        }else{
            $queryBuilder->update($this->configuration['table_name'])
            ->where("id = $checkRowId")
            ->set('order_id','?')
            ->set('encoded_message','?')
            ->set('headers','?')
            ->set('provider_name','?')
            ->set('template_name','?')
            ->set('handled','?')
            ->set('created_at','?')
            ->set('available_at','?');
        }
        

        
        $queryBuilder->setParameters([
            $orderId,
            $encodedMessage,
            json_encode($headers),
            $this->configuration['provider_name'],
            $template_name,
            0,
            $now,
            $availableAt
        ], [
            null,
            null,
            null,
            null,
            null,
            null,
            Types::DATETIME_MUTABLE,
            Types::DATETIME_MUTABLE
        ]);
        
        $sql = $queryBuilder->getSQL();

        $this->executeStatement($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes());

        return $this->driverConnection->lastInsertId();
    }

    public function get(): ?array
    {
        get:
        $this->driverConnection->beginTransaction();
        try {
            $query = $this->createAvailableMessagesQueryBuilder()
                ->andWhere("push_sent = 0")
                ->andWhere("email_sent = 0")
                ->andWhere("handled = 0")
                ->orderBy('available_at', 'ASC')
                ->setMaxResults(1);

            // Append pessimistic write lock to FROM clause if db platform supports it
            $sql = $query->getSQL();
            if (($fromPart = $query->getQueryPart('from')) &&
                ($table = $fromPart[0]['table'] ?? null) &&
                ($alias = $fromPart[0]['alias'] ?? null)
            ) {
                $fromClause = sprintf('%s %s', $table, $alias);
                $sql = str_replace(
                    sprintf('FROM %s WHERE', $fromClause),
                    sprintf('FROM %s WHERE', $this->driverConnection->getDatabasePlatform()->appendLockHint($fromClause, LockMode::PESSIMISTIC_WRITE)),
                    $sql
                );
            }

            // use SELECT ... FOR UPDATE to lock table
            $stmt = $this->executeQuery(
                $sql.' '.$this->driverConnection->getDatabasePlatform()->getWriteLockSQL(),
                $query->getParameters(),
                $query->getParameterTypes()
            );
            $doctrineEnvelope = $stmt instanceof Result || $stmt instanceof DriverResult ? $stmt->fetchAssociative() : $stmt->fetch();
            
            if (false === $doctrineEnvelope) {
                $this->driverConnection->commit();
                $this->queueEmptiedAt = microtime(true) * 1000;
                $this->logger->info('Current consuming cycle has been reached the end. Waiting for next message...');
                $this->receiveTimeout($this->configuration['receive_timeout']);
                return null;
            }
            // Postgres can "group" notifications having the same channel and payload
            // We need to be sure to empty the queue before blocking again
            $this->queueEmptiedAt = null;

            $doctrineEnvelope = $this->decodeEnvelopeHeaders($doctrineEnvelope);

            $queryBuilder = $this->driverConnection->createQueryBuilder()
                ->update($this->configuration['table_name'])
                ->set('delivered_at', '?')
                ->where('id = ?');
            $now = new \DateTime();
            $this->executeStatement($queryBuilder->getSQL(), [
                $now,
                $doctrineEnvelope['id'],
            ], [
                Types::DATETIME_MUTABLE,
            ]);

            $this->driverConnection->commit();

            return $doctrineEnvelope;
        } catch (\Throwable $e) {
            $this->driverConnection->rollBack();

            if ($this->autoSetup && $e instanceof TableNotFoundException) {
                $this->setup();
                goto get;
            }

            throw $e;
        }
    }

    public function ack(string $id): bool
    {
        try {
            return $this->driverConnection->update($this->configuration['table_name'], ['handled' => true ],['id' => $id]) > 0;
        } catch (DBALException | Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function reject(string $id): bool
    {
        try {
            return $this->driverConnection->update($this->configuration['table_name'], ['handled' => true ],['id' => $id]) > 0;
        } catch (DBALException | Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function setup(): void
    {
        $configuration = $this->driverConnection->getConfiguration();
        $assetFilter = $configuration->getSchemaAssetsFilter();
        //$configuration->setSchemaAssetsFilter(null);
        //$this->updateSchema();
        $configuration->setSchemaAssetsFilter($assetFilter);
        $this->autoSetup = false;
    }

    public function getMessageCount(): int
    {
        $queryBuilder = $this->createAvailableMessagesQueryBuilder()
            ->select('COUNT(m.id) as message_count')
            ->setMaxResults(1);

        $stmt = $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes());

        return $stmt instanceof Result || $stmt instanceof DriverResult ? $stmt->fetchOne() : $stmt->fetchColumn();
    }

    public function findAll(int $limit = null): array
    {
        $queryBuilder = $this->createAvailableMessagesQueryBuilder();
        if (null !== $limit) {
            $queryBuilder->setMaxResults($limit);
        }

        $stmt = $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes());
        $data = $stmt instanceof Result || $stmt instanceof DriverResult ? $stmt->fetchAllAssociative() : $stmt->fetchAll();

        return array_map(function ($doctrineEnvelope) {
            return $this->decodeEnvelopeHeaders($doctrineEnvelope);
        }, $data);
    }

    public function find($id): ?array
    {
        $queryBuilder = $this->createQueryBuilder()
            ->where('m.id = ?');

        $stmt = $this->executeQuery($queryBuilder->getSQL(), [$id]);
        $data = $stmt instanceof Result || $stmt instanceof DriverResult ? $stmt->fetchAssociative() : $stmt->fetch();

        return false === $data ? null : $this->decodeEnvelopeHeaders($data);
    }

    /**
     * @internal
     */
    public function configureSchema(Schema $schema, DBALConnection $forConnection): void
    {
        // only update the schema for this connection
        if ($forConnection !== $this->driverConnection) {
            return;
        }

        if ($schema->hasTable($this->configuration['table_name'])) {
            return;
        }

        //$this->addTableToSchema($schema);
    }

    /**
     * @internal
     */
    public function getExtraSetupSqlForTable(Table $createdTable): array
    {
        return [];
    }

    private function createAvailableMessagesQueryBuilder(): QueryBuilder
    {
        $now = new \DateTime();
        $redeliverLimit = (clone $now)->modify(sprintf('-%d seconds', $this->configuration['redeliver_timeout']));

        return $this->createQueryBuilder()
            ->where('m.delivered_at is null OR m.delivered_at < ?')
            ->andWhere('m.available_at <= ?')
            ->andWhere('m.provider_name = ?')
            ->andWhere('m.template_name = ?')
            ->setParameters([
                $redeliverLimit,
                $now,
                $this->configuration['provider_name'],
                $this->configuration['template_name'],
            ], [
                Types::DATETIME_MUTABLE,
                Types::DATETIME_MUTABLE,
                null,
                null
            ]);
    }

    private function createQueryBuilder(): QueryBuilder
    {
        return $this->driverConnection->createQueryBuilder()
            ->select('m.*')
            ->from($this->configuration['table_name'], 'm');
    }

    private function createQueryBuilderCheckRow(int $orderId, string $template_name): int
    {
        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->select('*')
            ->from($this->configuration['table_name'])
            ->where('order_id = ?')
            ->andWhere('template_name = ?')
            ->setParameters([
                $orderId,
                $template_name,
            ])
            ->setMaxResults(1);
        
        $result = $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes());

        return $result instanceof Result || $result instanceof DriverResult ? $result->fetchOne() : $result->fetchColumn();
    }

    private function executeQuery(string $sql, array $parameters = [], array $types = [])
    {
        try {
            $stmt = $this->driverConnection->executeQuery($sql, $parameters, $types);
        } catch (TableNotFoundException $e) {
            if ($this->driverConnection->isTransactionActive()) {
                throw $e;
            }

            // create table
            if ($this->autoSetup) {
                $this->setup();
            }
            $stmt = $this->driverConnection->executeQuery($sql, $parameters, $types);
        }

        return $stmt;
    }

    protected function executeStatement(string $sql, array $parameters = [], array $types = [])
    {
        try {
            if (method_exists($this->driverConnection, 'executeStatement')) {
                $stmt = $this->driverConnection->executeStatement($sql, $parameters, $types);
            } else {
                $stmt = $this->driverConnection->executeUpdate($sql, $parameters, $types);
            }
        } catch (TableNotFoundException $e) {
            if ($this->driverConnection->isTransactionActive()) {
                throw $e;
            }

            // create table
            if ($this->autoSetup) {
                $this->setup();
            }
            if (method_exists($this->driverConnection, 'executeStatement')) {
                $stmt = $this->driverConnection->executeStatement($sql, $parameters, $types);
            } else {
                $stmt = $this->driverConnection->executeUpdate($sql, $parameters, $types);
            }
        }

        return $stmt;
    }

    private function getSchema(): Schema
    {
        //
    }

    private function addTableToSchema(Schema $schema): void
    {
        //
    }

    private function decodeEnvelopeHeaders(array $doctrineEnvelope): array
    {
        $doctrineEnvelope['headers'] = json_decode($doctrineEnvelope['headers'], true);

        return $doctrineEnvelope;
    }

    private function updateSchema(): void
    {
        //
    }

    public function receiveTimeout(int $timeout_ms): void
    {
        if ($timeout_ms > 0) {
            usleep($timeout_ms * 1000);
        }
        return; // todo:
    }
}
