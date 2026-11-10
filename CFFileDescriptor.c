/* OSXER APPS (file descriptor): one-shot readiness through libdispatch, delivered
 * by a CF run-loop source on the caller's run loop, never on the dispatch queue.
 * Sources retain the descriptor; its source cache is weak, avoiding an ownership
 * cycle. Monitoring uses private dup fds to avoid fd reuse races.
 * Contract: https://developer.apple.com/documentation/corefoundation/cffiledescriptor
 */
#include <CoreFoundation/CFFileDescriptor.h>
#include <CoreFoundation/CFArray.h>
#include <CoreFoundation/CFRuntime.h>
#include <CoreFoundation/CFPriv.h>
#include <stdlib.h>
#include <dispatch/dispatch.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

struct __CFFileDescriptor {
    CFRuntimeBase base;
    pthread_mutex_t lock;
    int fd;
    Boolean valid, closeOnInvalidate;
    CFOptionFlags armed, pending;
    CFFileDescriptorCallBack callback;
    CFFileDescriptorContext context;
    dispatch_queue_t queue;
    dispatch_source_t readSource, writeSource;
    CFRunLoopSourceRef source;
    CFMutableArrayRef loops;
};

struct FDSourceLink {
    CFFileDescriptorRef descriptor;
    CFRunLoopSourceRef source;
};

// Source destruction clears the weak cache before freeing its context. Under
// f->lock, _CFTryRetain can therefore safely reject a source being deallocated.
static void fdSourceRelease(const void *info) {
    struct FDSourceLink *link = (struct FDSourceLink *)info;
    CFFileDescriptorRef f = link->descriptor;
    pthread_mutex_lock(&f->lock);
    if (f->source == link->source) f->source = NULL;
    pthread_mutex_unlock(&f->lock);
    free(link);
    CFRelease(f);
}

static const char fdQueueKey;

static void fdFinalize(CFTypeRef object) {
    CFFileDescriptorRef f = (CFFileDescriptorRef)object;
    CFFileDescriptorInvalidate(f);
    // Event handlers do not retain f; cancellation plus draining their serial
    // queue makes releasing an unscheduled descriptor safe.
    if (f->queue) {
        if (dispatch_get_specific(&fdQueueKey) != f) dispatch_sync(f->queue, ^{});
        dispatch_release(f->queue);
    }
    if (f->loops) CFRelease(f->loops);
    pthread_mutex_destroy(&f->lock);
}

CFTypeID CFFileDescriptorGetTypeID(void) {
    static CFTypeID type;
    static dispatch_once_t once;
    dispatch_once(&once, ^{
        static const CFRuntimeClass cls = {0, "CFFileDescriptor", NULL, NULL, fdFinalize};
        type = _CFRuntimeRegisterClass(&cls);
    });
    return type;
}

static void fdReady(CFFileDescriptorRef f, CFOptionFlags bit) {
    CFRunLoopSourceRef source = NULL;
    CFArrayRef loops = NULL;
    pthread_mutex_lock(&f->lock);
    if (f->valid && (f->armed & bit)) {
        dispatch_suspend(bit == kCFFileDescriptorReadCallBack ? f->readSource : f->writeSource);
        f->armed &= ~bit;
        f->pending |= bit;
        if (f->source) source = (CFRunLoopSourceRef)_CFTryRetain(f->source);
        loops = CFArrayCreateCopy(NULL, f->loops);
    }
    pthread_mutex_unlock(&f->lock);
    if (source) {
        CFRunLoopSourceSignal(source);
        for (CFIndex i = 0; i < CFArrayGetCount(loops); ++i)
            CFRunLoopWakeUp((CFRunLoopRef)CFArrayGetValueAtIndex(loops, i));
        CFRelease(source);
    }
    if (loops) CFRelease(loops);
}

CFFileDescriptorRef CFFileDescriptorCreate(CFAllocatorRef allocator, int fd,
    Boolean closeOnInvalidate, CFFileDescriptorCallBack callback,
    const CFFileDescriptorContext *context) {
    if (fd < 0 || fcntl(fd, F_GETFD) < 0 || !callback || (context && context->version != 0)) return NULL;
    int readFD = dup(fd);
    if (readFD < 0) return NULL;
    int writeFD = dup(fd);
    if (writeFD < 0) { close(readFD); return NULL; }
    fcntl(readFD, F_SETFD, FD_CLOEXEC);
    fcntl(writeFD, F_SETFD, FD_CLOEXEC);
    CFFileDescriptorRef f = (CFFileDescriptorRef)_CFRuntimeCreateInstance(allocator,
        CFFileDescriptorGetTypeID(), sizeof(*f) - sizeof(CFRuntimeBase), NULL);
    if (!f) { close(readFD); close(writeFD); return NULL; }
    pthread_mutex_init(&f->lock, NULL);
    f->fd = fd;
    f->callback = callback;
    f->valid = true;
    f->closeOnInvalidate = false; // Ownership starts only after successful creation.
    f->loops = CFArrayCreateMutable(allocator, 0, &kCFTypeArrayCallBacks);
    if (!f->loops) { close(readFD); close(writeFD); CFRelease(f); return NULL; }
    f->queue = dispatch_queue_create("org.darling.CFFileDescriptor", DISPATCH_QUEUE_SERIAL);
    dispatch_queue_set_specific(f->queue, &fdQueueKey, f, NULL);
    f->readSource = dispatch_source_create(DISPATCH_SOURCE_TYPE_READ, readFD, 0, f->queue);
    f->writeSource = dispatch_source_create(DISPATCH_SOURCE_TYPE_WRITE, writeFD, 0, f->queue);
    if (context) {
        f->context = *context;
        if (context->retain) f->context.info = context->retain(context->info);
    }
    // Each cancellation handler owns only its duplicate, not the descriptor.
    if (f->readSource) {
        dispatch_source_set_cancel_handler(f->readSource, ^{ close(readFD); });
        dispatch_source_set_event_handler(f->readSource, ^{ fdReady(f, kCFFileDescriptorReadCallBack); });
    } else close(readFD);
    if (f->writeSource) {
        dispatch_source_set_cancel_handler(f->writeSource, ^{ close(writeFD); });
        dispatch_source_set_event_handler(f->writeSource, ^{ fdReady(f, kCFFileDescriptorWriteCallBack); });
    } else close(writeFD);
    if (!f->readSource || !f->writeSource) {
        f->closeOnInvalidate = false; // Failed creation does not take fd ownership.
        CFRelease(f);
        return NULL;
    }
    f->closeOnInvalidate = closeOnInvalidate;
    return f;
}

CFFileDescriptorNativeDescriptor CFFileDescriptorGetNativeDescriptor(CFFileDescriptorRef f) {
    if (!f) return -1;
    pthread_mutex_lock(&f->lock);
    int fd = f->valid ? f->fd : -1;
    pthread_mutex_unlock(&f->lock);
    return fd;
}
Boolean CFFileDescriptorIsValid(CFFileDescriptorRef f) { return CFFileDescriptorGetNativeDescriptor(f) >= 0; }
void CFFileDescriptorGetContext(CFFileDescriptorRef f, CFFileDescriptorContext *context) {
    if (!f || !context) return;
    pthread_mutex_lock(&f->lock);
    *context = f->context;
    pthread_mutex_unlock(&f->lock);
}
void CFFileDescriptorEnableCallBacks(CFFileDescriptorRef f, CFOptionFlags types) {
    if (!f) return;
    pthread_mutex_lock(&f->lock);
    if (f->valid) {
        CFOptionFlags enable = types & ~f->armed & 3;
        if (enable & kCFFileDescriptorReadCallBack) dispatch_resume(f->readSource);
        if (enable & kCFFileDescriptorWriteCallBack) dispatch_resume(f->writeSource);
        f->armed |= enable;
    }
    pthread_mutex_unlock(&f->lock);
}
void CFFileDescriptorDisableCallBacks(CFFileDescriptorRef f, CFOptionFlags types) {
    if (!f) return;
    pthread_mutex_lock(&f->lock);
    if (f->valid) {
        CFOptionFlags disable = types & f->armed;
        if (disable & kCFFileDescriptorReadCallBack) dispatch_suspend(f->readSource);
        if (disable & kCFFileDescriptorWriteCallBack) dispatch_suspend(f->writeSource);
        f->armed &= ~types;
        f->pending &= ~types;
    }
    pthread_mutex_unlock(&f->lock);
}
void CFFileDescriptorInvalidate(CFFileDescriptorRef f) {
    if (!f) return;
    pthread_mutex_lock(&f->lock);
    if (!f->valid) { pthread_mutex_unlock(&f->lock); return; }
    f->valid = false;
    CFRunLoopSourceRef source = f->source ? (CFRunLoopSourceRef)_CFTryRetain(f->source) : NULL;
    f->source = NULL;
    CFFileDescriptorContext context = f->context;
    memset(&f->context, 0, sizeof(f->context));
    dispatch_source_t sources[] = {f->readSource, f->writeSource};
    for (unsigned i = 0; i < 2; ++i) {
        if (!sources[i]) continue;
        dispatch_source_cancel(sources[i]);
        if (!(f->armed & (1UL << i))) dispatch_resume(sources[i]);
        dispatch_release(sources[i]);
    }
    f->readSource = f->writeSource = NULL;
    f->armed = f->pending = 0;
    if (f->closeOnInvalidate) close(f->fd);
    f->fd = -1;
    pthread_mutex_unlock(&f->lock);
    // Removing sources invokes cancel callbacks, so never do it under our lock.
    if (source) { CFRunLoopSourceInvalidate(source); CFRelease(source); }
    if (context.release) context.release(context.info);
}

static void fdSchedule(void *info, CFRunLoopRef loop, CFStringRef mode) {
    CFFileDescriptorRef f = ((struct FDSourceLink *)info)->descriptor;
    pthread_mutex_lock(&f->lock);
    CFArrayAppendValue(f->loops, loop);
    Boolean pending = f->pending != 0;
    if (pending && f->source) CFRunLoopSourceSignal(f->source);
    pthread_mutex_unlock(&f->lock);
    if (pending) CFRunLoopWakeUp(loop);
}
static void fdCancel(void *info, CFRunLoopRef loop, CFStringRef mode) {
    CFFileDescriptorRef f = ((struct FDSourceLink *)info)->descriptor;
    pthread_mutex_lock(&f->lock);
    CFIndex index = CFArrayGetFirstIndexOfValue(f->loops, CFRangeMake(0, CFArrayGetCount(f->loops)), loop);
    if (index != kCFNotFound) CFArrayRemoveValueAtIndex(f->loops, index);
    pthread_mutex_unlock(&f->lock);
}
static void fdPerform(void *info) {
    CFFileDescriptorRef f = ((struct FDSourceLink *)info)->descriptor;
    pthread_mutex_lock(&f->lock);
    CFOptionFlags pending = f->valid ? f->pending : 0;
    f->pending = 0;
    CFFileDescriptorContext context = f->context;
    if (pending && context.retain) context.info = context.retain(context.info);
    pthread_mutex_unlock(&f->lock);
    if (pending) {
        f->callback(f, pending, context.info);
        if (context.retain && context.release) context.release(context.info);
    }
}
CFRunLoopSourceRef CFFileDescriptorCreateRunLoopSource(CFAllocatorRef allocator,
    CFFileDescriptorRef f, CFIndex order) {
    if (!f) return NULL;
    CFRunLoopSourceRef source = NULL, oldSource = NULL;
    struct FDSourceLink *failedLink = NULL;
    pthread_mutex_lock(&f->lock);
    if (f->valid) {
        if (f->source) source = (CFRunLoopSourceRef)_CFTryRetain(f->source);
        if (source && !CFRunLoopSourceIsValid(source)) {
            oldSource = source;
            source = NULL;
        }
        if (!source) {
            struct FDSourceLink *link = calloc(1, sizeof(*link));
            if (link) {
                link->descriptor = (CFFileDescriptorRef)CFRetain(f);
                CFRunLoopSourceContext context = {0, link, NULL, fdSourceRelease, NULL,
                    NULL, NULL, fdSchedule, fdCancel, fdPerform};
                source = CFRunLoopSourceCreate(allocator, order, &context);
                link->source = source;
                if (!source) failedLink = link;
            }
            f->source = source; // weak: the returned +1 belongs to the caller
        }
    }
    pthread_mutex_unlock(&f->lock);
    if (oldSource) CFRelease(oldSource);
    if (failedLink) fdSourceRelease(failedLink);
    return source;
}
