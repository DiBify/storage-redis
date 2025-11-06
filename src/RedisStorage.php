<?php
/**
 * Date: 22.02.2024
 * @author Timur Kasumov (XAKEPEHOK)
 */

namespace DiBify\Storage\Redis;

use DiBify\DiBify\Repository\Storage\StorageData;
use DiBify\DiBify\Repository\Storage\StorageInterface;
use Redis;

abstract class RedisStorage implements StorageInterface
{

    protected Redis $redis;

    public function __construct(Redis $redis)
    {
        $this->redis = $redis;
    }

    public function findById(string $id): ?StorageData
    {
        $json = $this->redis->get($this->getRedisKey($id));
        if (empty($json)) {
            return null;
        }

        $data = json_decode($json, true, JSON_THROW_ON_ERROR);
        return new StorageData(
            $id,
            $data,
            $this->scope()
        );
    }

    public function findByIds($ids): array
    {
        $ids = array_values($ids);
        $values = $this->redis->mGet(array_map(
            fn(string $id) => $this->getRedisKey($id),
            $ids
        ));

        $result = [];
        foreach ($ids as $index => $id) {
            $json = $values[$index];
            if ($json === false) {
                continue;
            }
            $result[$id] = new StorageData(
                $id,
                json_decode($json, true, JSON_THROW_ON_ERROR),
                $this->scope()
            );
        }

        return $result;
    }

    public function insert(StorageData $data, array $options = []): void
    {
        $redisOptions = [];
        if ($this->ttl() > 0) {
            $redisOptions['ex'] = $this->ttl();
        }

        $this->redis->set(
            $this->getRedisKey($data->id, $options['scope'] ?? null),
            json_encode($data->body),
            $redisOptions
        );
    }

    public function update(StorageData $data, array $options = []): void
    {
        $this->insert($data, $options);
    }

    public function delete(string $id, array $options = []): void
    {
        $this->redis->del($this->getRedisKey($id, $options['scope'] ?? null));
    }

    abstract public function keyPrefix(): string;

    abstract public function scope(): ?string;

    protected function ttl(): ?int
    {
        return null;
    }

    protected function getRedisKey(string $id, ?string $scope = null): string
    {
        return implode(':', array_filter([
            $this->keyPrefix(),
            $scope ?? $this->scope(),
            $id
        ]));
    }

    public function freeUpMemory(): void
    {
    }
}