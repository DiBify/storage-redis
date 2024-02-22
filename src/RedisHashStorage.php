<?php
/**
 * Date: 22.02.2024
 * @author Timur Kasumov (XAKEPEHOK)
 */

namespace DiBify\Storage\Redis;

use DiBify\DiBify\Repository\Storage\StorageData;
use DiBify\DiBify\Repository\Storage\StorageInterface;
use Redis;

abstract class RedisHashStorage implements StorageInterface
{

    private Redis $redis;

    public function __construct(Redis $redis)
    {
        $this->redis = $redis;
    }

    public function findById(string $id): ?StorageData
    {
        $json = $this->redis->hGet($this->getRedisKey(), $id);
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
        $values = $this->redis->hMGet($this->getRedisKey(), $ids);

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
        $this->redis->hSet(
            $this->getRedisKey($options['scope'] ?? null),
            $data->id,
            json_encode($data->body),
        );
    }

    public function update(StorageData $data, array $options = []): void
    {
        $this->insert($data, $options);
    }

    public function delete(string $id, array $options = []): void
    {
        $this->redis->hDel($this->getRedisKey($options['scope'] ?? null), $id);
    }

    abstract public function keyPrefix(): string;

    abstract public function scope(): ?string;

    protected function getRedisKey(?string $scope = null): string
    {
        return implode(':', array_filter([
            $this->keyPrefix(),
            $scope ?? $this->scope(),
        ]));
    }

    public function freeUpMemory(): void
    {
    }
}