<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EmrecanMuslu\Messenger\Transport\NotificationTransport;

use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
use Doctrine\Persistence\ConnectionRegistry;
use Symfony\Bridge\Doctrine\ManagerRegistry;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * @author Vincent Touzet <vincent.touzet@gmail.com>
 */
class CustomDoctrineTransportFactory implements TransportFactoryInterface
{
    private $registry;

    public function __construct($registry)
    {
        if (!$registry instanceof ManagerRegistry && !$registry instanceof ConnectionRegistry) {
            throw new \TypeError(sprintf('Expected an instance of "%s" or "%s", but got "%s".', ManagerRegistry::class, ConnectionRegistry::class, get_debug_type($registry)));
        }

        $this->registry = $registry;
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $useNotify = ($options['use_notify'] ?? true);
        unset($options['transport_name'], $options['use_notify']);
        // Always allow PostgreSQL-specific keys, to be able to transparently fallback to the native driver when LISTEN/NOTIFY isn't available
        $configuration = CustomPostgreSqlConnection::buildConfiguration($dsn, $options);

        try {
            $driverConnection = $this->registry->getConnection($configuration['connection']);
        } catch (\InvalidArgumentException $e) {
            throw new TransportException(sprintf('Could not find Doctrine connection from Messenger DSN "%s".', $dsn), 0, $e);
        }

        if ($useNotify && $driverConnection->getDriver() instanceof AbstractPostgreSQLDriver) {
            $connection = new CustomPostgreSqlConnection($configuration, $driverConnection);
        } else {
            $connection = new Connection($configuration, $driverConnection);
        }

        return new DoctrineTransport($connection, $serializer);
    }

    public function supports(string $dsn, array $options): bool
    {
        return 0 === strpos($dsn, 'notificationsms://');
    }
}
