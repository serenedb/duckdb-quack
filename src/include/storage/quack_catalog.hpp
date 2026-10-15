//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/mutex.hpp"
#include "storage/quack_schema.hpp"
#include "quack_periodic_worker.hpp"
#include "quack_uri.hpp"

namespace duckdb {

class QuackCatalog;
class QuackClient;
class QuackClientConnection;

class QuackCatalog : public Catalog {
public:
	explicit QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri_p, ClientContext &context, string token,
	                      string client_id, idx_t heartbeat_timeout_seconds);
	~QuackCatalog() override;

public:
	string GetCatalogType() override {
		return "quack";
	}
	//! The schema an unqualified name means on the server
	optional<Identifier> GetDefaultSchema() const override {
		return default_schema;
	}
	static QuackCatalog &GetQuackCatalog(ClientContext &context, Value &catalog_name);
	static bool IsQuackScan(const string &name);
	bool SupportsPushdown(const TableRef &ref) override;
	void Initialize(bool load_builtin) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	bool Supports(RemoteCapability capability) const override {
		switch (capability) {
		case RemoteCapability::IS_REMOTE:
		case RemoteCapability::EXECUTE_QUERY_NODE:
		case RemoteCapability::EXECUTE_STATEMENT:
		case RemoteCapability::CONNECT:
			return true;
		default:
			return false;
		}
	}
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<SQLStatement> statement) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	string GetConnectDisplay() override {
		return GetDBPath();
	}

	unique_ptr<ColumnDataCollection> ExecuteCommandInternal(ClientContext &context, const string &query);
	const QuackUri &GetServerUri();

	//! The server session of `context`. Every local session gets its own, so their statements and transactions
	//! do not run into each other on the server.
	shared_ptr<QuackClientConnection> GetClientConnection(ClientContext &context);

	void Refresh(ClientContext &context);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	//! Build the "quack_query_by_name" table function call that runs "sql" on the server. Statements pushed
	//! down by the RemotePushdownOptimizer change the remote catalog, so they ask for a catalog refresh
	unique_ptr<TableRef> CreateRemoteQueryRef(const string &sql, bool refresh_catalog, bool use_transaction = true);

	QuackLoadCatalogData LoadCatalog(ClientContext &context);
	void SendHeartbeats();

private:
	QuackUri server_uri;
	string token;
	string client_id;
	idx_t heartbeat_timeout_seconds;
	//! Tells this ATTACH apart from the others in the sessions a local context holds
	string attach_id;
	mutex sessions_lock;
	//! The sessions handed out, kept alive by one heartbeat for all of them
	vector<weak_ptr<QuackClientConnection>> sessions;
	QuackPeriodicWorker heartbeat;
	unique_ptr<QuackSchemaSet> schemas;
	Identifier default_schema = Identifier(DEFAULT_SCHEMA);
};

} // namespace duckdb
