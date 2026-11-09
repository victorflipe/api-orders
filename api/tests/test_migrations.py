from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext

from database import Base


async def test_migrations_match_the_models(engine):
    """Falha se um modelo mudou sem a migration correspondente (ou o contrário)."""

    def diff(sync_conn):
        context = MigrationContext.configure(sync_conn, opts={"compare_type": True})
        return compare_metadata(context, Base.metadata)

    async with engine.connect() as conn:
        assert await conn.run_sync(diff) == []
