#include <string.h>
#include <node.h>

#include "macros.h"
#include "database.h"
#include "statement.h"

using namespace node_sqlite3;

Persistent<FunctionTemplate> Database::constructor_template;
Persistent<FunctionTemplate> Blob::blob_constructor_template;


void Database::Init(Handle<Object> target) {
    NanScope();

    Local<FunctionTemplate> t = NanNew<FunctionTemplate>(New);

    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(NanNew("Database"));

    NODE_SET_PROTOTYPE_METHOD(t, "close", Close);
    NODE_SET_PROTOTYPE_METHOD(t, "exec", Exec);
    //NODE_SET_PROTOTYPE_METHOD(t, "openBlob", OpenBlob);
    //NODE_SET_PROTOTYPE_METHOD(t, "closeBlob", CloseBlob);
    NODE_SET_PROTOTYPE_METHOD(t, "wait", Wait);
    NODE_SET_PROTOTYPE_METHOD(t, "loadExtension", LoadExtension);
    NODE_SET_PROTOTYPE_METHOD(t, "serialize", Serialize);
    NODE_SET_PROTOTYPE_METHOD(t, "parallelize", Parallelize);
    NODE_SET_PROTOTYPE_METHOD(t, "configure", Configure);

    NODE_SET_GETTER(t, "open", OpenGetter);

    NanAssignPersistent(constructor_template, t);

    target->Set(NanNew("Database"),
        t->GetFunction());
}

void Database::Process() {
    NanScope();

    if (!open && locked && !queue.empty()) {
        EXCEPTION(NanNew<String>("Database handle is closed"), SQLITE_MISUSE, exception);
        Local<Value> argv[] = { exception };
        bool called = false;

        // Call all callbacks with the error object.
        while (!queue.empty()) {
            Call* call = queue.front();
            Local<Function> cb = NanNew(call->baton->callback);
            if (!cb.IsEmpty() && cb->IsFunction()) {
                TRY_CATCH_CALL(NanObjectWrapHandle(this), cb, 1, argv);
                called = true;
            }
            queue.pop();
            // We don't call the actual callback, so we have to make sure that
            // the baton gets destroyed.
            delete call->baton;
            delete call;
        }

        // When we couldn't call a callback function, emit an error on the
        // Database object.
        if (!called) {
            Local<Value> args[] = { NanNew("error"), exception };
            EMIT_EVENT(NanObjectWrapHandle(this), 2, args);
        }
        return;
    }

    while (open && (!locked || pending == 0) && !queue.empty()) {
        Call* call = queue.front();

        if (call->exclusive && pending > 0) {
            break;
        }

        queue.pop();
        locked = call->exclusive;
        call->callback(call->baton);
        delete call;

        if (locked) break;
    }
}

void Database::Schedule(Work_Callback callback, Baton* baton, bool exclusive) {
    NanScope();
    if (!open && locked) {
        EXCEPTION(NanNew<String>("Database is closed"), SQLITE_MISUSE, exception);
        Local<Function> cb = NanNew(baton->callback);
        if (!cb.IsEmpty() && cb->IsFunction()) {
            Local<Value> argv[] = { exception };
            TRY_CATCH_CALL(NanObjectWrapHandle(this), cb, 1, argv);
        }
        else {
            Local<Value> argv[] = { NanNew("error"), exception };
            EMIT_EVENT(NanObjectWrapHandle(this), 2, argv);
        }
        return;
    }

    if (!open || ((locked || exclusive || serialize) && pending > 0)) {
        queue.push(new Call(callback, baton, exclusive || serialize));
    }
    else {
        locked = exclusive;
        callback(baton);
    }
}

NAN_METHOD(Database::New) {
    NanScope();

    if (!args.IsConstructCall()) {
        return NanThrowTypeError("Use the new operator to create new Database objects");
    }

    REQUIRE_ARGUMENT_STRING(0, filename);
    int pos = 1;

    int mode;
    if (args.Length() >= pos && args[pos]->IsInt32()) {
        mode = args[pos++]->Int32Value();
    } else {
        mode = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;
    }

    Local<Function> callback;
    if (args.Length() >= pos && args[pos]->IsFunction()) {
        callback = Local<Function>::Cast(args[pos++]);
    }

    Database* db = new Database();
    db->Wrap(args.This());

    args.This()->ForceSet(NanNew("filename"), args[0]->ToString(), ReadOnly);
    args.This()->ForceSet(NanNew("mode"), NanNew<Integer>(mode), ReadOnly);

    // Start opening the database.
    OpenBaton* baton = new OpenBaton(db, callback, *filename, mode);
    Work_BeginOpen(baton);

    NanReturnValue(args.This());
}

void Database::Work_BeginOpen(Baton* baton) {
    int status = uv_queue_work(uv_default_loop(),
        &baton->request, Work_Open, (uv_after_work_cb)Work_AfterOpen);
    assert(status == 0);
}

void Database::Work_Open(uv_work_t* req) {
    OpenBaton* baton = static_cast<OpenBaton*>(req->data);
    Database* db = baton->db;

    baton->status = sqlite3_open_v2(
        baton->filename.c_str(),
        &db->_handle,
        baton->mode,
        NULL
    );

    if (baton->status != SQLITE_OK) {
        baton->message = std::string(sqlite3_errmsg(db->_handle));
        sqlite3_close(db->_handle);
        db->_handle = NULL;
    }
    else {
        // Set default database handle values.
        sqlite3_busy_timeout(db->_handle, 1000);
    }
}

void Database::Work_AfterOpen(uv_work_t* req) {
    NanScope();
    OpenBaton* baton = static_cast<OpenBaton*>(req->data);
    Database* db = baton->db;

    Local<Value> argv[1];
    if (baton->status != SQLITE_OK) {
        EXCEPTION(NanNew<String>(baton->message.c_str()), baton->status, exception);
        argv[0] = exception;
    }
    else {
        db->open = true;
        argv[0] = NanNew(NanNull());
    }

    Local<Function> cb = NanNew(baton->callback);

    if (!cb.IsEmpty() && cb->IsFunction()) {
        TRY_CATCH_CALL(NanObjectWrapHandle(db), cb, 1, argv);
    }
    else if (!db->open) {
        Local<Value> args[] = { NanNew("error"), argv[0] };
        EMIT_EVENT(NanObjectWrapHandle(db), 2, args);
    }

    if (db->open) {
        Local<Value> args[] = { NanNew("open") };
        EMIT_EVENT(NanObjectWrapHandle(db), 1, args);
        db->Process();
    }

    delete baton;
}

NAN_GETTER(Database::OpenGetter) {
    NanScope();
    Database* db = ObjectWrap::Unwrap<Database>(args.This());
    NanReturnValue(NanNew<Boolean>(db->open));
}

NAN_METHOD(Database::Close) {
    NanScope();
    Database* db = ObjectWrap::Unwrap<Database>(args.This());
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    Baton* baton = new Baton(db, callback);
    db->Schedule(Work_BeginClose, baton, true);

    NanReturnValue(args.This());
}

void Database::Work_BeginClose(Baton* baton) {
    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_handle);
    assert(baton->db->pending == 0);

    baton->db->RemoveCallbacks();
    int status = uv_queue_work(uv_default_loop(),
        &baton->request, Work_Close, (uv_after_work_cb)Work_AfterClose);
    assert(status == 0);
}

void Database::Work_Close(uv_work_t* req) {
    Baton* baton = static_cast<Baton*>(req->data);
    Database* db = baton->db;

    baton->status = sqlite3_close(db->_handle);

    if (baton->status != SQLITE_OK) {
        baton->message = std::string(sqlite3_errmsg(db->_handle));
    }
    else {
        db->_handle = NULL;
    }
}

void Database::Work_AfterClose(uv_work_t* req) {
    NanScope();
    Baton* baton = static_cast<Baton*>(req->data);
    Database* db = baton->db;

    Local<Value> argv[1];
    if (baton->status != SQLITE_OK) {
        EXCEPTION(NanNew<String>(baton->message.c_str()), baton->status, exception);
        argv[0] = exception;
    }
    else {
        db->open = false;
        // Leave db->locked to indicate that this db object has reached
        // the end of its life.
        argv[0] = NanNew(NanNull());
    }

    Local<Function> cb = NanNew(baton->callback);

    // Fire callbacks.
    if (!cb.IsEmpty() && cb->IsFunction()) {
        TRY_CATCH_CALL(NanObjectWrapHandle(db), cb, 1, argv);
    }
    else if (db->open) {
        Local<Value> args[] = { NanNew("error"), argv[0] };
        EMIT_EVENT(NanObjectWrapHandle(db), 2, args);
    }

    if (!db->open) {
        Local<Value> args[] = { NanNew("close"), argv[0] };
        EMIT_EVENT(NanObjectWrapHandle(db), 1, args);
        db->Process();
    }

    delete baton;
}

NAN_METHOD(Database::Serialize) {
    NanScope();
    Database* db = ObjectWrap::Unwrap<Database>(args.This());
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    bool before = db->serialize;
    db->serialize = true;

    if (!callback.IsEmpty() && callback->IsFunction()) {
        TRY_CATCH_CALL(args.This(), callback, 0, NULL);
        db->serialize = before;
    }

    db->Process();

    NanReturnValue(args.This());
}

NAN_METHOD(Database::Parallelize) {
    NanScope();
    Database* db = ObjectWrap::Unwrap<Database>(args.This());
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    bool before = db->serialize;
    db->serialize = false;

    if (!callback.IsEmpty() && callback->IsFunction()) {
        TRY_CATCH_CALL(args.This(), callback, 0, NULL);
        db->serialize = before;
    }

    db->Process();

    NanReturnValue(args.This());
}

NAN_METHOD(Database::Configure) {
    NanScope();
    Database* db = ObjectWrap::Unwrap<Database>(args.This());

    REQUIRE_ARGUMENTS(2);

    if (args[0]->Equals(NanNew("trace"))) {
        Local<Function> handle;
        Baton* baton = new Baton(db, handle);
        db->Schedule(RegisterTraceCallback, baton);
    }
    else if (args[0]->Equals(NanNew("profile"))) {
        Local<Function> handle;
        Baton* baton = new Baton(db, handle);
        db->Schedule(RegisterProfileCallback, baton);
    }
    else if (args[0]->Equals(NanNew("busyTimeout"))) {
        if (!args[1]->IsInt32()) {
            return NanThrowTypeError("Value must be an integer");
        }
        Local<Function> handle;
        Baton* baton = new Baton(db, handle);
        baton->status = args[1]->Int32Value();
        db->Schedule(SetBusyTimeout, baton);
    }
    else {
        return NanThrowError(Exception::Error(String::Concat(
            args[0]->ToString(),
            NanNew<String>(" is not a valid configuration option")
        )));
    }

    db->Process();

    NanReturnValue(args.This());
}

void Database::SetBusyTimeout(Baton* baton) {
    assert(baton->db->open);
    assert(baton->db->_handle);

    // Abuse the status field for passing the timeout.
    sqlite3_busy_timeout(baton->db->_handle, baton->status);

    delete baton;
}

void Database::RegisterTraceCallback(Baton* baton) {
    assert(baton->db->open);
    assert(baton->db->_handle);
    Database* db = baton->db;

    if (db->debug_trace == NULL) {
        // Add it.
        db->debug_trace = new AsyncTrace(db, TraceCallback);
        sqlite3_trace(db->_handle, TraceCallback, db);
    }
    else {
        // Remove it.
        sqlite3_trace(db->_handle, NULL, NULL);
        db->debug_trace->finish();
        db->debug_trace = NULL;
    }

    delete baton;
}

void Database::TraceCallback(void* db, const char* sql) {
    // Note: This function is called in the thread pool.
    // Note: Some queries, such as "EXPLAIN" queries, are not sent through this.
    static_cast<Database*>(db)->debug_trace->send(new std::string(sql));
}

void Database::TraceCallback(Database* db, std::string* sql) {
    // Note: This function is called in the main V8 thread.
    NanScope();
    Local<Value> argv[] = {
        NanNew("trace"),
        NanNew<String>(sql->c_str())
    };
    EMIT_EVENT(NanObjectWrapHandle(db), 2, argv);
    delete sql;
}

void Database::RegisterProfileCallback(Baton* baton) {
    assert(baton->db->open);
    assert(baton->db->_handle);
    Database* db = baton->db;

    if (db->debug_profile == NULL) {
        // Add it.
        db->debug_profile = new AsyncProfile(db, ProfileCallback);
        sqlite3_profile(db->_handle, ProfileCallback, db);
    }
    else {
        // Remove it.
        sqlite3_profile(db->_handle, NULL, NULL);
        db->debug_profile->finish();
        db->debug_profile = NULL;
    }

    delete baton;
}

void Database::ProfileCallback(void* db, const char* sql, sqlite3_uint64 nsecs) {
    // Note: This function is called in the thread pool.
    // Note: Some queries, such as "EXPLAIN" queries, are not sent through this.
    ProfileInfo* info = new ProfileInfo();
    info->sql = std::string(sql);
    info->nsecs = nsecs;
    static_cast<Database*>(db)->debug_profile->send(info);
}

void Database::ProfileCallback(Database *db, ProfileInfo* info) {
    NanScope();
    Local<Value> argv[] = {
        NanNew("profile"),
        NanNew<String>(info->sql.c_str()),
        NanNew<Number>((double)info->nsecs / 1000000.0)
    };
    EMIT_EVENT(NanObjectWrapHandle(db), 3, argv);
    delete info;
}

void Database::RegisterUpdateCallback(Baton* baton) {
    assert(baton->db->open);
    assert(baton->db->_handle);
    Database* db = baton->db;

    if (db->update_event == NULL) {
        // Add it.
        db->update_event = new AsyncUpdate(db, UpdateCallback);
        sqlite3_update_hook(db->_handle, UpdateCallback, db);
    }
    else {
        // Remove it.
        sqlite3_update_hook(db->_handle, NULL, NULL);
        db->update_event->finish();
        db->update_event = NULL;
    }

    delete baton;
}

void Database::UpdateCallback(void* db, int type, const char* database,
        const char* table, sqlite3_int64 rowid) {
    // Note: This function is called in the thread pool.
    // Note: Some queries, such as "EXPLAIN" queries, are not sent through this.
    UpdateInfo* info = new UpdateInfo();
    info->type = type;
    info->database = std::string(database);
    info->table = std::string(table);
    info->rowid = rowid;
    static_cast<Database*>(db)->update_event->send(info);
}

void Database::UpdateCallback(Database *db, UpdateInfo* info) {
    NanScope();

    Local<Value> argv[] = {
        NanNew(sqlite_authorizer_string(info->type)),
        NanNew<String>(info->database.c_str()),
        NanNew<String>(info->table.c_str()),
        NanNew<Number>(info->rowid),
    };
    EMIT_EVENT(NanObjectWrapHandle(db), 4, argv);
    delete info;
}

NAN_METHOD(Database::Exec) {
    NanScope();
    Database* db = ObjectWrap::Unwrap<Database>(args.This());

    REQUIRE_ARGUMENT_STRING(0, sql);
    OPTIONAL_ARGUMENT_FUNCTION(1, callback);

    Baton* baton = new ExecBaton(db, callback, *sql);
    db->Schedule(Work_BeginExec, baton, true);

    NanReturnValue(args.This());
}

void Database::Work_BeginExec(Baton* baton) {
    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_handle);
    assert(baton->db->pending == 0);
    int status = uv_queue_work(uv_default_loop(),
        &baton->request, Work_Exec, (uv_after_work_cb)Work_AfterExec);
    assert(status == 0);
}

void Database::Work_Exec(uv_work_t* req) {
    ExecBaton* baton = static_cast<ExecBaton*>(req->data);

    char* message = NULL;
    baton->status = sqlite3_exec(
        baton->db->_handle,
        baton->sql.c_str(),
        NULL,
        NULL,
        &message
    );

    if (baton->status != SQLITE_OK && message != NULL) {
        baton->message = std::string(message);
        sqlite3_free(message);
    }
}

void Database::Work_AfterExec(uv_work_t* req) {
    NanScope();
    ExecBaton* baton = static_cast<ExecBaton*>(req->data);
    Database* db = baton->db;

    Local<Function> cb = NanNew(baton->callback);

    if (baton->status != SQLITE_OK) {
        EXCEPTION(NanNew<String>(baton->message.c_str()), baton->status, exception);

        if (!cb.IsEmpty() && cb->IsFunction()) {
            Local<Value> argv[] = { exception };
            TRY_CATCH_CALL(NanObjectWrapHandle(db), cb, 1, argv);
        }
        else {
            Local<Value> args[] = { NanNew("error"), exception };
            EMIT_EVENT(NanObjectWrapHandle(db), 2, args);
        }
    }
    else if (!cb.IsEmpty() && cb->IsFunction()) {
        Local<Value> argv[] = { NanNew(NanNull()) };
        TRY_CATCH_CALL(NanObjectWrapHandle(db), cb, 1, argv);
    }

    db->Process();

    delete baton;
}

/*********************************************************/
void Blob::Init(Handle<Object> target) {
    NanScope();

    Local<FunctionTemplate> t = NanNew<FunctionTemplate>(New);

    t->InstanceTemplate()->SetInternalFieldCount(1);
    t->SetClassName(NanNew("Blob"));

    NODE_SET_PROTOTYPE_METHOD(t, "close", Close);
    NanAssignPersistent(blob_constructor_template, t);
    target->Set(NanNew("Blob"),
        t->GetFunction());
}


NAN_METHOD(Blob::New) {
    NanScope();
    if (!args.IsConstructCall()) {
        return NanThrowTypeError("Use the new operator to create new Blob objects");
    }

    int length = args.Length();

    if (length <= 0 || !Database::HasInstance(args[0])) {
        return NanThrowTypeError("Database object expected");
    }

    REQUIRE_ARGUMENT_STRING(1, table);
    REQUIRE_ARGUMENT_STRING(2, column);

    Database* db = ObjectWrap::Unwrap<Database>(args[0]->ToObject());

    Blob* blob = new Blob(db);

    //stmt->Wrap(args.This());
    
    // if (args.Length() <= 3 || !args[3]->IsUint32()) {                        
    //     return NanThrowTypeError("Argument 3 must be a rowid");          
    // }                                                                          
    // sqlite3_int64 rowid = args[3]->Uint32Value();

    // if (args.Length() <= 4 || !args[4]->IsInt32()) {                        
    //     return NanThrowTypeError("Argument 4 must be an integer");          
    // }                                                                          
    // int flags = args[4]->Int32Value();

    // Baton* baton = new OpenBlobBaton(db, callback, *msg);
    // db->Schedule(Work_BeginOpenBlob, baton, true);
    // Going to insert a zeroblob of the size of the file
    char insert_sql[1024];
    snprintf(insert_sql, sizeof(insert_sql), "INSERT INTO %s (%s) VALUES (?)", *table, *column);

    sqlite3_stmt *insert_stmt;
    int rc = sqlite3_prepare_v2(db->_handle, insert_sql, -1, &insert_stmt, NULL);

    if(SQLITE_OK != rc) {
        char err[500];
        sprintf(err, "%s, Can't prepare insert. Err = %i", insert_sql, rc);
        return NanThrowError(err);
    }

    // Bind a block of zeros the size of the file we're going to insert later
    sqlite3_bind_zeroblob(insert_stmt, 1, 100000);
    if(SQLITE_DONE != (rc = sqlite3_step(insert_stmt))) {
        return NanThrowError("Insert statement didn't work");
    }
    sqlite3_finalize(insert_stmt);
    sqlite3_int64 rowid = sqlite3_last_insert_rowid(db->_handle);
    //printf("Created a row, id %i, with a blank blob size %i\n", (int)rowid, (int)filesize);

    rc = sqlite3_blob_open(db->_handle, "main", *table, *column, rowid, 1, &(blob->blob));
    if(SQLITE_OK != rc) {
        return NanThrowError("Couldn't get blob handle");
    }

    blob->Wrap(args.This());

    NanReturnValue(args.This());
}

NAN_METHOD(Blob::Close) {
    NanScope();
    Blob* blob = ObjectWrap::Unwrap<Blob>(args.This());

    sqlite3_blob_close(blob->db->blob);
}

void Database::Work_BeginOpenBlob(Baton* baton) {
    assert(baton->db->open);
    assert(baton->db->_handle);
    assert(baton->db->pending == 0);
    int status = uv_queue_work(uv_default_loop(),
        &baton->request, Work_OpenBlob, (uv_after_work_cb)Work_AfterOpenBlob);
    assert(status == 0);
}

void Database::Work_OpenBlob(uv_work_t* req) {
    OpenBlobBaton* baton = static_cast<OpenBlobBaton*>(req->data);
    // char* message = NULL;
    // // baton->status = sqlite3_exec(
    //     baton->db->_handle,
    //     baton->sql.c_str(),
    //     NULL,
    //     NULL,
    //     &message
    // );

    // if (baton->status != SQLITE_OK && message != NULL) {
    //     baton->message = std::string(message);
    //     sqlite3_free(message);
    // }
}

void Database::Work_AfterOpenBlob(uv_work_t* req) {
    NanScope();
    OpenBlobBaton* baton = static_cast<OpenBlobBaton*>(req->data);
    Database* db = baton->db;

    // Local<Function> cb = NanNew(baton->callback);

    // if (baton->status != SQLITE_OK) {
    //     EXCEPTION(NanNew<String>(baton->message.c_str()), baton->status, exception);

    //     if (!cb.IsEmpty() && cb->IsFunction()) {
    //         Local<Value> argv[] = { exception };
    //         TRY_CATCH_CALL(NanObjectWrapHandle(db), cb, 1, argv);
    //     }
    //     else {
    //         Local<Value> args[] = { NanNew("error"), exception };
    //         EMIT_EVENT(NanObjectWrapHandle(db), 2, args);
    //     }
    // }
    // else if (!cb.IsEmpty() && cb->IsFunction()) {
    //     Local<Value> argv[] = { NanNew(NanNull()) };
    //     TRY_CATCH_CALL(NanObjectWrapHandle(db), cb, 1, argv);
    // }

    NanThrowError(baton->msg.c_str());
    db->Process();
    //NanReturnValue(13);

    delete baton;
}

/*********************************************************/

NAN_METHOD(Database::Wait) {
    NanScope();
    Database* db = ObjectWrap::Unwrap<Database>(args.This());

    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    Baton* baton = new Baton(db, callback);
    db->Schedule(Work_Wait, baton, true);

    NanReturnValue(args.This());
}

void Database::Work_Wait(Baton* baton) {
    NanScope();

    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_handle);
    assert(baton->db->pending == 0);

    Local<Function> cb = NanNew(baton->callback);
    if (!cb.IsEmpty() && cb->IsFunction()) {
        Local<Value> argv[] = { NanNew(NanNull()) };
        TRY_CATCH_CALL(NanObjectWrapHandle(baton->db), cb, 1, argv);
    }

    baton->db->Process();

    delete baton;
}

NAN_METHOD(Database::LoadExtension) {
    NanScope();
    Database* db = ObjectWrap::Unwrap<Database>(args.This());

    REQUIRE_ARGUMENT_STRING(0, filename);
    OPTIONAL_ARGUMENT_FUNCTION(1, callback);

    Baton* baton = new LoadExtensionBaton(db, callback, *filename);
    db->Schedule(Work_BeginLoadExtension, baton, true);

    NanReturnValue(args.This());
}

void Database::Work_BeginLoadExtension(Baton* baton) {
    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_handle);
    assert(baton->db->pending == 0);
    int status = uv_queue_work(uv_default_loop(),
        &baton->request, Work_LoadExtension, (uv_after_work_cb)Work_AfterLoadExtension);
    assert(status == 0);
}

void Database::Work_LoadExtension(uv_work_t* req) {
    LoadExtensionBaton* baton = static_cast<LoadExtensionBaton*>(req->data);

    sqlite3_enable_load_extension(baton->db->_handle, 1);

    char* message = NULL;
    baton->status = sqlite3_load_extension(
        baton->db->_handle,
        baton->filename.c_str(),
        0,
        &message
    );

    sqlite3_enable_load_extension(baton->db->_handle, 0);

    if (baton->status != SQLITE_OK && message != NULL) {
        baton->message = std::string(message);
        sqlite3_free(message);
    }
}

void Database::Work_AfterLoadExtension(uv_work_t* req) {
    NanScope();
    LoadExtensionBaton* baton = static_cast<LoadExtensionBaton*>(req->data);
    Database* db = baton->db;
    Local<Function> cb = NanNew(baton->callback);

    if (baton->status != SQLITE_OK) {
        EXCEPTION(NanNew<String>(baton->message.c_str()), baton->status, exception);

        if (!cb.IsEmpty() && cb->IsFunction()) {
            Local<Value> argv[] = { exception };
            TRY_CATCH_CALL(NanObjectWrapHandle(db), cb, 1, argv);
        }
        else {
            Local<Value> args[] = { NanNew("error"), exception };
            EMIT_EVENT(NanObjectWrapHandle(db), 2, args);
        }
    }
    else if (!cb.IsEmpty() && cb->IsFunction()) {
        Local<Value> argv[] = { NanNew(NanNull()) };
        TRY_CATCH_CALL(NanObjectWrapHandle(db), cb, 1, argv);
    }

    db->Process();

    delete baton;
}

void Database::RemoveCallbacks() {
    if (debug_trace) {
        debug_trace->finish();
        debug_trace = NULL;
    }
    if (debug_profile) {
        debug_profile->finish();
        debug_profile = NULL;
    }
}
