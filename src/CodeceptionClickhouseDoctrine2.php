<?php
declare(strict_types=1);

namespace Zhulanov111\CodeceptionClickhouseDoctrine2;

use Codeception\Exception\ModuleConfigException;
use Codeception\Lib\Interfaces\DependsOnModule;
use Codeception\Module;
use Codeception\TestInterface;
use Codeception\Util\Shared\Asserts;
use Zhulanov111\CodeceptionClickhouseDoctrine2\Common\EntityNormalizer;
use Zhulanov111\CodeceptionClickhouseDoctrine2\Exception\NotImplementedException;
use Doctrine\ORM\EntityManagerInterface;
use Psr\Container\ContainerInterface;

final class CodeceptionClickhouseDoctrine2 extends Module implements DependsOnModule
{
    use Asserts;

    private ?Module\Symfony $symfonyModule = null;

    /* Clickhouse entity manager */
    private ?EntityManagerInterface $chEM = null;

    private EntityNormalizer $entityNormalizer;

    /* 'em' - name of entity manager */
    protected $requiredFields = ['em'];

    protected $config = [
        'em' => null,
        'cleanup' => false
    ];

    public function _depends()
    {
        return [
            Module\Symfony::class => 'Module Symfony is required'
        ];
    }

    public function _inject(Module\Symfony $symfonyModule)
    {
        $this->symfonyModule = $symfonyModule;
    }

    public function _initialize()
    {
        /** @var ContainerInterface $symfonyContainer */
        $symfonyContainer = $this->symfonyModule->_getContainer();

        $serviceID = $this->getChEMServiceID($this->getConfEM());

        if (!$symfonyContainer->has($serviceID)) {
            throw new ModuleConfigException(
                $this,
                sprintf('Passed em - "%s" is not found', $this->getConfEM())
            );
        }

        $this->chEM = $symfonyContainer->get($serviceID);

        $this->entityNormalizer = new EntityNormalizer();
    }

    /**
     * @throws \Doctrine\DBAL\Exception
     */
    public function chHaveInRepository($classNameOrInstance, array $data = []): void
    {
        if (is_object($classNameOrInstance)) {
            $className = get_class($classNameOrInstance);

            $_data = $this
                ->entityNormalizer
                ->toArray(
                    $this->chEM->getClassMetadata($className),
                    $classNameOrInstance
                );
        } elseif (is_string($classNameOrInstance)) {
            $className = $classNameOrInstance;
            $_data = $data;
        } else {
            throw new \InvalidArgumentException(
                sprintf(
                    'CodeceptionClickhouseDoctrine2::haveInRepository expects a class name or instance as first argument, got "%s" instead',
                    gettype($classNameOrInstance)
                )
            );
        }

        $sql = sprintf(
            'INSERT INTO %s (%s) VALUES (%s)',
            $this
                ->chEM
                ->getClassMetadata($className)
                ->getTableName()
                ,
            implode(',', array_keys($_data)),
            ':' . implode(', :', array_keys($_data))
        );

        $this
            ->chEM
            ->getConnection()
            ->prepare($sql)
            ->executeStatement($_data)
        ;
    }

    public function chHaveFakeRepository(string $className, array $methods = []): void
    {
        throw new NotImplementedException();
    }

    public function chLoadFixtures($fixtures, bool $append = true): void
    {
        throw new NotImplementedException();
    }

    /**
     * @throws \Doctrine\DBAL\Exception
     */
    public function chSeeInRepository(string $entity, $params = []): void
    {
        $result = $this->proceedSeeInRepository($entity, $params);

        $this->assert($result);
    }

    /**
     * @throws \Doctrine\DBAL\Exception
     */
    public function chDontSeeInRepository(string $entity, $params = []): void
    {
        $result = $this->proceedSeeInRepository($entity, $params);

        $this->assertNot($result);
    }

    public function chGrabFromRepository($entity, $field, $params = [])
    {
        throw new NotImplementedException();
    }

    public function chGrabEntitiesFromRepository(string $entity, array $params = []): array
    {
        throw new NotImplementedException();
    }

    public function chGrabEntityFromRepository(string $entity, array $params = [])
    {
        throw new NotImplementedException();
    }

    public function _before(TestInterface $test): void
    {
        if ($this->config['cleanup']) {
            $this->clickhouseCleanup();
        }
    }

    private function clickhouseCleanup(): void
    {
        $conn = $this->chEM->getConnection();

        $db = $conn->getDatabase();

        $stmt = $conn->executeQuery(sprintf('SHOW TABLES FROM %s', $db));

        while ($row = $stmt->fetchAssociative()) {
            $sql = sprintf('TRUNCATE TABLE %s.%s', $db, $row['name']);

            try {
                $conn->executeStatement($sql);
            } catch (\Throwable $throwable) {

            }
        }
    }

    private function getChEMServiceID(string $em): string
    {
        return sprintf('doctrine.orm.%s_entity_manager', $em);
    }

    /**
     * @throws \Doctrine\DBAL\Exception
     */
    private function proceedSeeInRepository(string $entity, $params = []): array
    {
        $tableName = $this
            ->chEM
            ->getClassMetadata($entity)
            ->getTableName();

        $conn = $this->chEM->getConnection();

        $qb = $conn->createQueryBuilder();

        $qb->select('*');
        $qb->from($tableName);

        foreach ($params as $paramKey => $paramVal) {
            $qb->andWhere(
                $qb->expr()->eq(
                    $paramKey,
                    is_string($paramVal) ? sprintf("'%s'", $paramVal) : $paramVal
                )
            );
        }

        $stmt = $conn->executeQuery($qb->getSQL());

        return ['True', (bool) $stmt->fetchAssociative()];
    }

    private function getConfEM(): ?string
    {
        return $this->config['em'] ?? null;
    }
}
