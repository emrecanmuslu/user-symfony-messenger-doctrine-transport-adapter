<?php

namespace EmrecanMuslu\Messenger\Transport\NotificationTransport;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

class DoctrineReceivedStamp implements NonSendableStampInterface
{
    private $id;

    public function __construct(string $id)
    {
        $this->id = $id;
    }

    public function getId(): string
    {
        return $this->id;
    }
}
