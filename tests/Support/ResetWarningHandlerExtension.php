<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Support;

use BabelQueue\Codec\EnvelopeCodec;
use PHPUnit\Event\Test\Finished;
use PHPUnit\Event\Test\FinishedSubscriber;
use PHPUnit\Runner\Extension\Extension;
use PHPUnit\Runner\Extension\Facade;
use PHPUnit\Runner\Extension\ParameterCollection;
use PHPUnit\TextUI\Configuration\Configuration;

/**
 * Resets the process-global {@see EnvelopeCodec::setWarningHandler()} sink after every test, so a
 * test that installs a handler (and fails before restoring it) can never leak it into the next test
 * and make the suite order-dependent.
 */
final class ResetWarningHandlerExtension implements Extension
{
    public function bootstrap(Configuration $configuration, Facade $facade, ParameterCollection $parameters): void
    {
        $facade->registerSubscriber(new class () implements FinishedSubscriber {
            public function notify(Finished $event): void
            {
                EnvelopeCodec::setWarningHandler(null);
            }
        });
    }
}
