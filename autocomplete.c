#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <glob.h>
#include <errno.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <unicode/uloc.h>
#include <unicode/utypes.h>
#include <unicode/ustring.h>
#include "uthash.h"
#include "utstring.h"
#include "json/json.h"
#include <signal.h>
#include <time.h>
#include <event.h>
#include <evhttp.h>

#define NAME "autocomplete"
#define VERSION "0.1"
#define DEBUG 1
#define DEFAULT_PORT 8080

#define key_match(x) (strncmp(((struct el*)x)->key,key,strlen(key)) == 0)

#define safe_free(s)    \
if (s) {                \
    free(s);            \
}

#if defined __APPLE__ && defined __MACH__
#define GLOBC(g) g.gl_matchc
#else
#define GLOBC(g) g.gl_pathc
#endif

typedef struct el {
    char *key;
    char *id;
    char *type;
    char *data;
    time_t when;
    int count;
    UT_hash_handle hh;  /* handle for key hash */
    UT_hash_handle rh;  /* handle for results hash */
} el;

struct namespace {
    char *name;
    int nelems;
    int dirty;
    struct el *elems;
    UT_hash_handle hh;
};

struct evhttp *httpd;
struct namespace *spaces = NULL;
int max_elems = 1000;
char *default_locale = ULOC_US;
char *db_dir = NULL;
struct event backup_timer;
struct timeval backup_tv = {10, 0};

void put_cb(struct evhttp_request *req, void *arg);
void search_cb(struct evhttp_request *req, void *arg);
void incr_cb(struct evhttp_request *req, void *arg);
void decr_cb(struct evhttp_request *req, void *arg);
void del_cb(struct evhttp_request *req, void *arg);

struct namespace *get_namespace(char *namespace)
{
    struct namespace *ns = NULL;
    HASH_FIND_STR(spaces, namespace, ns);
    return ns;
}

int key_sort(el *a, el *b) {
    return strcasecmp(a->key, b->key);
}

int time_count_sort(el *a, el *b) {
    if (a->when > b->when) {
        return -1;
    } else if (a->when < b->when) {
        return 1;
    } else {
        if (a->count > b->count) {
            return -1;
        } else if (a->count < b->count) {
            return 1;
        }
    }
    return 0;
}

int time_count_rev_sort(el *a, el *b) {
    if (a->when > b->when) {
        return 1;
    } else if (a->when < b->when) {
        return -1;
    } else {
        if (a->count > b->count) {
            return 1;
        } else if (a->count < b->count) {
            return -1;
        }
    }
    return 0;
}

void free_el(struct el *e)
{
    if (e) {
        if (e->key) {
            free(e->key);
        }
        free(e);
    }
}

char *utf8_tolower(char *s, char *locale)
{
    UChar *buf = NULL;
    char *buf2 = NULL;
    UErrorCode err = U_ZERO_ERROR;
    int32_t len, len2;

    u_strFromUTF8(NULL, 0, &len, s, -1, &err);
    buf = malloc(sizeof(UChar) * len+1);
    err = U_ZERO_ERROR;
    u_strFromUTF8(buf, len+1, &len, s, -1, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "u_strFromUTF8 failed: %s: %s\n", s, u_errorName(err));
        free(buf);
        return NULL;
    }

    err = U_ZERO_ERROR;
    len2 = u_strToLower(NULL, 0, (UChar *)buf, -1, locale, &err);
    if (len2 > len) {
        buf = realloc(buf, len2+1);
    }
    err = U_ZERO_ERROR;
    u_strToLower(buf, len2+1, (UChar *)buf, -1, locale, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "u_strToLower failed: %s: %s\n", s, u_errorName(err));
        free(buf);
        return NULL;
    }

    err = U_ZERO_ERROR;
    u_strToUTF8(NULL, 0, &len, buf, -1, &err);
    buf2 = malloc(sizeof(char) * len+1);
    err = U_ZERO_ERROR;
    u_strToUTF8(buf2, len+1, &len, (UChar *)buf, -1, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "u_strToUTF8 failed: %s: %s\n", s, u_errorName(err));
        free(buf);
        free(buf2);
        return NULL;
    }
    free(buf);
    return buf2;
}

struct el *put_el(char *locale, char *namespace, char *key, char *data, char *id, char *type, time_t when, int mark_dirty)
{
    struct namespace *ns;
    struct el *e;
    char *key_nrm = utf8_tolower(key, locale);

    ns = get_namespace(namespace);
    if (!ns) {
        ns = malloc(sizeof(*ns));
        memset(ns, 0, sizeof(*ns));
        ns->name = strdup(namespace);
        HASH_ADD_KEYPTR(hh, spaces, ns->name, strlen(ns->name), ns);
    }
    if (mark_dirty) {
        ns->dirty = 1;
    }
    if (HASH_COUNT(ns->elems) > max_elems) {
        /*
         *  This is tricky. UT_hash keeps two sort orders.
         *  This deletes from the head ( oldest insert ).
         */
        e = ns->elems;
        HASH_DEL(ns->elems, e);
        free_el(e);
    }
    fprintf(stderr, "looking for %s\n", key);
    HASH_FIND_STR(ns->elems, key_nrm, e);
    if (e) {
        fprintf(stderr, "delete %s\n", e->key);
        HASH_DEL(ns->elems, e);
        safe_free(key_nrm);
    } else {
        e = malloc(sizeof(*e));
        memset(e, 0, sizeof(*e));
        e->key = key_nrm;
    }
    safe_free(e->data);
    e->data = (data ? strdup(data) : NULL);
    safe_free(e->id);
    e->id = (id ? strdup(id) : NULL);
    safe_free(e->type);
    e->type = (type ? strdup(type) : NULL);
    e->when = when;
    HASH_ADD_KEYPTR(hh, ns->elems, e->key, strlen(e->key), e);

    return e;
}

char *gen_path(UT_string *buf, char *dir, char *file, char *ext)
{
    utstring_clear(buf);
    utstring_printf(buf, "%s/%s%s", dir, file, ext);
    return utstring_body(buf);
}

void backup(int timer_fd, short event, void *arg)
{
    struct namespace *ns;
    struct el *e;
    UT_string *path1, *path2;
    int fd, ok, n;
    struct hdr {
        uint32_t klen;
        uint32_t dlen;
        uint32_t ilen;
        uint32_t tlen;
        uint32_t when;
        uint32_t count;
    } hdr;

    evtimer_set(&backup_timer, backup, NULL);
    evtimer_add(&backup_timer, &backup_tv);

    if (!db_dir) {
        return;
    }
    utstring_new(path1);
    utstring_new(path2);
    for (ns=spaces; ns != NULL; ns=ns->hh.next) {
        if (!ns->dirty) {
            continue;
        }
        ns->dirty = 0;
        fd = open(gen_path(path1, db_dir, ns->name, ".tmp"), O_CREAT|O_TRUNC|O_RDWR, 0660);
        if (fd == -1) {
            fprintf(stderr, "Open failed: %s: %s\n", utstring_body(path1), strerror(errno));
            continue;
        }
        fprintf(stderr, "Backing up: %s\n", ns->name);

        for (ok=1, e=ns->elems; e != NULL; e=e->hh.next) {
            /*
             * key sz, data sz, time, count, key, data, id, type
             */
            hdr.klen = htonl(strlen(e->key)+1);
            hdr.dlen = htonl(strlen(e->data)+1);
            hdr.ilen = htonl(strlen(e->id)+1);
            hdr.tlen = htonl(strlen(e->type)+1);
            hdr.when = htonl(e->when);
            hdr.count = htonl(e->count);
            n = write(fd, &hdr, sizeof(hdr));
            if (n == -1) {
                fprintf(stderr, "Write failed: %s: %s\n", utstring_body(path1), strerror(errno));
                ok = 0;
                break;
            }
            write(fd, e->key, strlen(e->key)+1);
            write(fd, e->data, strlen(e->data)+1);
            write(fd, e->id, strlen(e->id)+1);
            write(fd, e->type, strlen(e->type)+1);
        }
        if (ok) {
            rename(utstring_body(path1), gen_path(path2, db_dir, ns->name, ".bak"));
        }
        close(fd);
    }
    utstring_free(path1);
    utstring_free(path2);
}

void load()
{
    struct el *e;
    UT_string *ustr;
    glob_t g;
    int i, fd, n, klen, dlen, ilen, tlen;
    char *s, *namespace, *key = NULL, *id = NULL, *type = NULL, *data = NULL;
    struct hdr {
        uint32_t klen;
        uint32_t dlen;
        uint32_t ilen;
        uint32_t tlen;
        uint32_t when;
        uint32_t count;
    } hdr;

    if (!db_dir) {
        return;
    }

    utstring_new(ustr);
    utstring_printf(ustr, "%s/*.bak", db_dir);
    glob(utstring_body(ustr), 0, NULL, &g);
    for (i=0; i < GLOBC(g); i++) {
        fd = open(g.gl_pathv[i], O_RDONLY);
        if (fd == -1) {
            fprintf(stderr, "Open failed: %s: %s\n", g.gl_pathv[i], strerror(errno));
            continue;
        }
        namespace = strrchr(g.gl_pathv[i], '/')+1;
        s = strrchr(namespace, '.');
        *s = '\0';

        n = read(fd, &hdr, sizeof(hdr));
        fprintf(stderr, "Loading: %s\n", namespace);
        while (n == sizeof(hdr)) {
            klen = ntohl(hdr.klen);
            dlen = ntohl(hdr.dlen);
            ilen = ntohl(hdr.ilen);
            tlen = ntohl(hdr.tlen);
            key = realloc(key, klen);
            data = realloc(data, dlen);
            id = realloc(id, ilen);
            type = realloc(type, tlen);
            n = read(fd, key, klen);
            n = read(fd, data, dlen);
            n = read(fd, id, ilen);
            n = read(fd, type, tlen);
            e = put_el(NULL, namespace, key, data, id, type, ntohl(hdr.when), 0);
            e->count = ntohl(hdr.count);
            n = read(fd, &hdr, sizeof(hdr));
        }
        close(fd);
    }
    safe_free(key);
    safe_free(data);
    safe_free(id);
    safe_free(type);
    globfree(&g);
    utstring_free(ustr);
}

void put_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    struct el *e;
    char *namespace, *key, *id, *type, *data, *ts, *locale;
    time_t when = time(NULL);

    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    key = (char *)evhttp_find_header(&args, "key");
    data = (char *)evhttp_find_header(&args, "data");
    id = (char *)evhttp_find_header(&args, "id");
    type = (char *)evhttp_find_header(&args, "type");
    ts = (char *)evhttp_find_header(&args, "ts");
    if (ts) {
        when = (time_t)strtol(ts, NULL, 10);
    }
    locale = (char *)evhttp_find_header(&args, "locale");
    if (DEBUG) {
        fprintf(stderr, "/put_cb %s %s\n", namespace, key);
    }

    if (namespace && key) {
        e = put_el(locale, namespace, key, data, id, type, when, 1);
        e->count += 1;
        evhttp_send_reply(req, HTTP_OK, "OK", buf);
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }

    evhttp_clear_headers(&args);
    evbuffer_free(buf);
}

void del_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    char *namespace, *locale, *qkey, *key = NULL;
    struct el *e;
    struct namespace *ns;

    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    locale = (char *)evhttp_find_header(&args, "locale");
    qkey = (char *)evhttp_find_header(&args, "key");
    if (qkey) {
        key = utf8_tolower(qkey, locale);
    }

    if (DEBUG) {
        fprintf(stderr, "/search_cb %s %s\n", namespace, key);
    }

    if (namespace && key) {
        ns = get_namespace(namespace);
        if (ns) {
            HASH_FIND_STR(ns->elems, key, e);
            if (e) {
                HASH_DEL(ns->elems, e);
                free_el(e);
                evhttp_send_reply(req, HTTP_OK, "OK", buf);
            } else {
                evhttp_send_reply(req, HTTP_NOTFOUND, "KEY_NOT_FOUND", buf);
            }
        } else {
            evhttp_send_reply(req, HTTP_NOTFOUND, "NAMESPACE_NOT_FOUND", buf);
        }
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }

    safe_free(key);
    evhttp_clear_headers(&args);
    evbuffer_free(buf);
}

void incr_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    char *namespace, *locale, *svalue, *qkey, *key = NULL;
    struct el *e;
    int value = 0;
    struct namespace *ns;

    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    locale = (char *)evhttp_find_header(&args, "locale");
    qkey = (char *)evhttp_find_header(&args, "key");
    if (qkey) {
        key = utf8_tolower(qkey, locale);
    }
    svalue = (char *)evhttp_find_header(&args, "value");
    if (svalue) {
        value = atoi(svalue);
        if (arg) {  // this is a decr
            value = -value;
        }
    }

    if (DEBUG) {
        fprintf(stderr, "/incr_cb %s %s\n", namespace, key);
    }

    if (namespace && key && value) {
        ns = get_namespace(namespace);
        if (ns) {
            HASH_FIND_STR(ns->elems, key, e);
            if (e) {
                e->count += value;
                if (e->count <= 0) {
                    HASH_DEL(ns->elems, e);
                    free_el(e);
                }
                evhttp_send_reply(req, HTTP_OK, "OK", buf);
            } else {
                evhttp_send_reply(req, HTTP_NOTFOUND, "KEY_NOT_FOUND", buf);
            }
        } else {
            evhttp_send_reply(req, HTTP_NOTFOUND, "NAMESPACE_NOT_FOUND", buf);
        }
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }

    safe_free(key);
    evhttp_clear_headers(&args);
    evbuffer_free(buf);
}

void search_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    char *namespace, *slimit, *locale, *qkey, *key = NULL;
    struct json_object *jsobj, *jsel, *jsresults;
    struct el *e, *results = NULL;
    struct namespace *ns;
    int i, limit = 100;

    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    locale = (char *)evhttp_find_header(&args, "locale");
    qkey = (char *)evhttp_find_header(&args, "key");
    if (qkey) {
        key = utf8_tolower(qkey, locale);
    }
    slimit = (char *)evhttp_find_header(&args, "limit");
    if (slimit) {
        limit = atoi(slimit);
    }

    if (DEBUG) {
        fprintf(stderr, "/search_cb %s %s\n", namespace, key);
    }

    if (namespace && key) {
        jsobj = json_object_new_object();
        jsresults = json_object_new_array();
        ns = get_namespace(namespace);
        if (ns) {
            /*
             *  UT_hash does not offer a reentrant select func, so we will
             *  define it such that we have access to the stack.
             */
            HASH_SELECT(rh, results, hh, ns->elems, key_match);
            HASH_SRT(rh, results, time_count_sort);
            for (e=results, i=0; e != NULL && i < limit; e=e->rh.next, i++) {
                jsel = json_object_new_object();
                json_object_object_add(jsel, "key", json_object_new_string(e->key));
                if (e->data != NULL) {
                    json_object_object_add(jsel, "data", json_object_new_string(e->data));
                }
                if (e->id != NULL) {
                    json_object_object_add(jsel, "id", json_object_new_string(e->id));
                }
                if (e->type != NULL) {
                    json_object_object_add(jsel, "type", json_object_new_string(e->type));
                }

                json_object_array_add(jsresults, jsel);
                fprintf(stderr, "elem key %s\n", e->key);
            }
            HASH_CLEAR(rh, results);
        }
        json_object_object_add(jsobj, "results", jsresults);
        evbuffer_add_printf(buf, "%s\n", (char *)json_object_to_json_string(jsobj));
        evhttp_send_reply(req, HTTP_OK, "OK", buf);
        json_object_put(jsobj);
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }

    safe_free(key);
    evhttp_clear_headers(&args);
    evbuffer_free(buf);
}

void termination_handler(int signum)
{
    fprintf(stdout, "Shutting down...\n");
    event_loopbreak();
}

int main(int argc, char **argv)
{
    int opt;
    int port = DEFAULT_PORT;
    char *address = "0.0.0.0";
    UErrorCode err = U_ZERO_ERROR;

    while((opt = getopt(argc, argv, "a:d:p:l:")) != -1) {
        switch(opt) {
            case 'a':
                address = optarg;
                break;
            case 'd':
                db_dir = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'l':
                default_locale = optarg;
                break;
            case '?':
                fprintf (stderr, "Unknown option: '-%c'\n", optopt);
                return 1;
        }
    }

    signal(SIGINT, termination_handler);
    signal(SIGQUIT, termination_handler);
    signal(SIGTERM, termination_handler);
    signal(SIGPIPE, SIG_IGN);

    uloc_setDefault(default_locale, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "Could not set default location: %s: %s\n", default_locale, u_errorName(err));
        exit(1);
    }

    event_init();
    load();
    backup(0,0,NULL);

    httpd = evhttp_start(address, port);
    if (httpd == NULL) {
        fprintf(stdout, "Could not listen on: %s:%d\n", address, port);
        return 1;
    }

    evhttp_set_cb(httpd, "/put", put_cb, NULL);
    evhttp_set_cb(httpd, "/del", del_cb, NULL);
    evhttp_set_cb(httpd, "/incr", incr_cb, NULL);
    evhttp_set_cb(httpd, "/decr", incr_cb, (void *)'d');
    evhttp_set_cb(httpd, "/search", search_cb, NULL);

    fprintf(stdout, "Starting %s (%s) listening on: %s:%d\n", NAME, VERSION, address, port);

    event_dispatch();
    evhttp_free(httpd);
    return 0;
}
