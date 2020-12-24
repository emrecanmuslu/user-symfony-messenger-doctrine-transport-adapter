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

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * @author Vincent Touzet <vincent.touzet@gmail.com>
 */
class CustomDoctrineReceivedStamp implements NonSendableStampInterface
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
class_alias(CustomDoctrineReceivedStamp::class, Symfony\Component\Messenger\Transport\Doctrine\CustomDoctrineReceivedStamp::class);
