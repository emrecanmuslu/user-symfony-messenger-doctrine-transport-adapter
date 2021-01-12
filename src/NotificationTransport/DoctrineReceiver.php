<?php

namespace EmrecanMuslu\Messenger\Transport\NotificationTransport;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Exception\RetryableException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class DoctrineReceiver implements ReceiverInterface, MessageCountAwareInterface, ListableReceiverInterface
{
    private const MAX_RETRIES = 3;
    private $retryingSafetyCounter = 0;
    private $connection;
    private $serializer;

    public function __construct(Connection $connection, SerializerInterface $serializer)
    {
        $this->connection = $connection;
        $this->serializer = $serializer;
    }

    /**
     * {@inheritdoc}
     */
    public function get(): iterable
    {
        try {
            $doctrineEnvelope = $this->connection->get();
            $this->retryingSafetyCounter = 0; // reset counter
        } catch (RetryableException $exception) {
            // Do nothing when RetryableException occurs less than "MAX_RETRIES"
            // as it will likely be resolved on the next call to get()
            // Problem with concurrent consumers and database deadlocks
            if (++$this->retryingSafetyCounter >= self::MAX_RETRIES) {
                $this->retryingSafetyCounter = 0; // reset counter
                throw new TransportException($exception->getMessage(), 0, $exception);
            }

            return [];
        } catch (DBALException | Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        if (null === $doctrineEnvelope) {
            return [];
        }

        return [$this->createEnvelopeFromData($doctrineEnvelope)];
    }

    /**
     * {@inheritdoc}
     */
    public function ack(Envelope $envelope): void
    {
        try {
            $this->connection->ack($this->findDoctrineReceivedStamp($envelope)->getId());
        } catch (DBALException | Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function reject(Envelope $envelope): void
    {
        try {
            $this->connection->reject($this->findDoctrineReceivedStamp($envelope)->getId());
        } catch (DBALException | Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getMessageCount(): int
    {
        try {
            return $this->connection->getMessageCount();
        } catch (DBALException | Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function all(int $limit = null): iterable
    {
        try {
            $doctrineEnvelopes = $this->connection->findAll($limit);
        } catch (DBALException | Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        foreach ($doctrineEnvelopes as $doctrineEnvelope) {
            yield $doctrineEnvelope;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function find($id): ?Envelope
    {
        try {
            $doctrineEnvelope = $this->connection->find($id);
        } catch (DBALException | Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        if (null === $doctrineEnvelope) {
            return null;
        }

        return $doctrineEnvelope;
    }

    private function findDoctrineReceivedStamp(Envelope $envelope): DoctrineReceivedStamp
    {
        /** @var DoctrineReceivedStamp|null $doctrineReceivedStamp */
        $doctrineReceivedStamp = $envelope->last(DoctrineReceivedStamp::class);

        if (null === $doctrineReceivedStamp) {
            throw new LogicException('No DoctrineReceivedStamp found on the Envelope.');
        }

        return $doctrineReceivedStamp;
    }

    private function createEnvelopeFromData(array $data): Envelope
    {
        $conf = $this->connection->getConfiguration();
        
        foreach ($conf['payload']['models'] as $fullyQualifiedPayloadClassName) {
            $data['headers']['type'] = $fullyQualifiedPayloadClassName;
            try {
                $envelope = $this->serializer->decode([
                    'body' => $data['encoded_message'],
                    'headers' => $data['headers'],
                ]);
                break;
            } catch (MessageDecodingFailedException $exception) {
                $this->connection->reject($data['id']);

                throw $exception;
            }

        }


        return $envelope->with(
            new DoctrineReceivedStamp($data['id']),
            new TransportMessageIdStamp($data['id'])
        );
    }
}
