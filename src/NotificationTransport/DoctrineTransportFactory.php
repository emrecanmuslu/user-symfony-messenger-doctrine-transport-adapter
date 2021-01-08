<?php

namespace EmrecanMuslu\Messenger\Transport\NotificationTransport;

use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
use Doctrine\Persistence\ConnectionRegistry;
use Doctrine\Persistence\ManagerRegistry;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class DoctrineTransportFactory implements TransportFactoryInterface
{
    private $registry;
    private $logger;

    public function __construct(ManagerRegistry $registry, LoggerInterface $logger)
    {
        if (!$registry instanceof ManagerRegistry && !$registry instanceof ConnectionRegistry) {
            throw new \TypeError(sprintf('Expected an instance of "%s" or "%s", but got "%s".', ManagerRegistry::class, ConnectionRegistry::class, get_debug_type($registry)));
        }

        $this->registry = $registry;
        $this->logger = $logger;
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $useNotify = ($options['use_notify'] ?? true);
        unset($options['transport_name'], $options['use_notify']);
        // Always allow PostgreSQL-specific keys, to be able to transparently fallback to the native driver when LISTEN/NOTIFY isn't available
        $configuration = PostgreSqlConnection::buildConfiguration($dsn, $options);

        try {
            $driverConnection = $this->registry->getConnection($configuration['connection']);
        } catch (\InvalidArgumentException $e) {
            throw new TransportException(sprintf('Could not find Doctrine connection from Messenger DSN "%s".', $dsn), 0, $e);
        }

        if ($useNotify && $driverConnection->getDriver() instanceof AbstractPostgreSQLDriver) {
            $connection = new PostgreSqlConnection($configuration, $driverConnection);
        } else {
            $connection = new Connection($configuration, $driverConnection, null, $this->logger);
        }

        return new DoctrineTransport($connection, $serializer);
    }

    public function supports(string $dsn, array $options): bool
    {
        return 0 === strpos($dsn, 'notificationSms://');
    }
}
