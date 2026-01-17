import logging
import asyncio
from typing import Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor
from services.redis_service import redis_service
from services.rds_service import rds_service

logger = logging.getLogger(__name__)


class ParallelFetchService:
    def __init__(self, max_workers: int = 10):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = asyncio.Semaphore(max_workers)
    
    async def fetch_user_from_redis(self, user_id: int) -> Optional[Dict]:
        """Fetch user data from Redis asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                redis_service.get_user_data,
                user_id
            )
            if result:
                result['source'] = 'redis'
                logger.debug(f"Redis returned data for user {user_id}")
            return result
        except Exception as e:
            logger.error(f"Error fetching user {user_id} from Redis: {e}")
            return None
    
    async def fetch_user_from_rds(self, user_id: int) -> Optional[Dict]:
        """Fetch user data from RDS asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                rds_service.get_user_by_id,
                user_id
            )
            if result:
                result['source'] = 'rds'
                logger.debug(f"RDS returned data for user {user_id}")
            return result
        except Exception as e:
            logger.error(f"Error fetching user {user_id} from RDS: {e}")
            return None
    
    async def fetch_user_parallel(self, user_id: int) -> Optional[Dict]:
        """
        Fetch user data from both Redis and RDS in parallel.
        Returns whichever responds first, with Redis preferred if both respond simultaneously.
        """
        async with self.semaphore:
            try:
                # Create tasks for both sources
                redis_task = asyncio.create_task(self.fetch_user_from_redis(user_id))
                rds_task = asyncio.create_task(self.fetch_user_from_rds(user_id))
                
                # Wait for the first one to complete
                done, pending = await asyncio.wait(
                    [redis_task, rds_task],
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Get the first result
                first_result = None
                for task in done:
                    result = task.result()
                    if result:
                        first_result = result
                        logger.info(f"User {user_id} fetched from {result['source']} (fastest)")
                        break
                
                # If first result is None, wait for the other task
                if not first_result:
                    for task in pending:
                        try:
                            result = await asyncio.wait_for(task, timeout=2.0)
                            if result:
                                first_result = result
                                logger.info(f"User {user_id} fetched from {result['source']} (fallback)")
                                break
                        except asyncio.TimeoutError:
                            logger.warning(f"Timeout waiting for fallback source for user {user_id}")
                
                # Cancel any remaining tasks
                for task in pending:
                    if not task.done():
                        task.cancel()
                
                return first_result
                
            except Exception as e:
                logger.error(f"Error in parallel fetch for user {user_id}: {e}")
                return None
    
    async def fetch_multiple_users_parallel(self, user_ids: List[int]) -> Dict:
        """
        Fetch multiple users in parallel, each with Redis/RDS race condition.
        Returns dict with found users and not_found user IDs.
        """
        try:
            # Create tasks for all user IDs
            tasks = [self.fetch_user_parallel(user_id) for user_id in user_ids]
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            found_users = []
            not_found = []
            
            for user_id, result in zip(user_ids, results):
                if isinstance(result, Exception):
                    logger.error(f"Exception fetching user {user_id}: {result}")
                    not_found.append(user_id)
                elif result:
                    found_users.append(result)
                else:
                    not_found.append(user_id)
            
            logger.info(f"Fetched {len(found_users)} users, {len(not_found)} not found")
            
            return {
                "users": found_users,
                "total": len(found_users),
                "not_found": not_found
            }
            
        except Exception as e:
            logger.error(f"Error in parallel fetch for multiple users: {e}")
            raise
    
    def shutdown(self):
        """Shutdown the executor"""
        self.executor.shutdown(wait=True)


# Singleton instance
parallel_fetch_service = ParallelFetchService(max_workers=20)