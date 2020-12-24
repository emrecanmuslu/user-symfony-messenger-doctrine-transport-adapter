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

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;


/**
 * @author Vincent Touzet <vincent.touzet@gmail.com>
 */
class CustomDoctrineSender implements SenderInterface
{
    private $customConnection;
    private $serializer;

    public function __construct(CustomConnection $customConnection, SerializerInterface $serializer = null)
    {
        $this->customConnection = $customConnection;
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    /**
     * {@inheritdoc}
     */
    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        /** @var DelayStamp|null $delayStamp */
        $delayStamp = $envelope->last(DelayStamp::class);
        $delay = null !== $delayStamp ? $delayStamp->getDelay() : 0;

        try {
            $id = $this->customConnection->send($envelope, $encodedMessage['body'], $encodedMessage['headers'] ?? [], $delay);
        } catch (DBALException | Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        return $envelope->with(new TransportMessageIdStamp($id));
    }
}
class_alias(CustomDoctrineSender::class, Symfony\Component\Messenger\Transport\Doctrine\CustomDoctrineSender::class);
