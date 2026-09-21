#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

static int mapping_test_open(const char *path, int flags, ...);
static int mapping_test_shm_open(const char *path, int flags, mode_t mode);
static void *mapping_test_mmap(void *addr, size_t bytes, int prot, int flags,
                               int fd, off_t offset);
static int mapping_test_close(int fd);

/* Intercept only this SDK translation unit to exercise permission failures
 * without a UB driver. Other SDK objects link from the normal client library. */
#define open mapping_test_open
#define shm_open mapping_test_shm_open
#define mmap mapping_test_mmap
#define close mapping_test_close
#include "../clients/c/vemb_v16_client_sdk.c"
#undef open
#undef shm_open
#undef mmap
#undef close

static struct {
    int open_errors[2];
    int mmap_errors[2];
    int open_flags[2];
    int open_calls;
    int shm_calls;
    int mmap_calls;
    int close_calls;
    int protection;
    int expected_shm;
    int active_fd;
} test_state;
static unsigned char mapped_data[128];

static void reset_mapping_test(int shm) {
    memset(&test_state, 0, sizeof(test_state));
    test_state.expected_shm = shm;
    test_state.active_fd = -1;
}

static int record_open(const char *path, int flags) {
    assert(strcmp(path, test_state.expected_shm ? "/cli-test" : "/dev/obmm_shmdev1") == 0);
    assert(test_state.active_fd == -1);
    int i = test_state.open_calls++;
    assert(i < 2);
    test_state.open_flags[i] = flags;
    if (test_state.open_errors[i]) {
        errno = test_state.open_errors[i];
        return -1;
    }
    test_state.active_fd = 40 + i;
    return test_state.active_fd;
}

static int mapping_test_open(const char *path, int flags, ...) {
    assert(!test_state.expected_shm);
    return record_open(path, flags);
}

static int mapping_test_shm_open(const char *path, int flags, mode_t mode) {
    assert(test_state.expected_shm);
    assert(!(flags & (O_CREAT | O_SYNC)));
    assert(mode == 0666);
    test_state.shm_calls++;
    return record_open(path, flags);
}

static void *mapping_test_mmap(void *addr, size_t bytes, int prot, int flags,
                               int fd, off_t offset) {
    assert(addr == NULL);
    assert(bytes == 81);
    assert(offset == sysconf(_SC_PAGESIZE));
    assert(flags == MAP_SHARED);
    assert(fd == test_state.active_fd && fd >= 0);
    int i = test_state.mmap_calls++;
    assert(i < 2);
    test_state.protection = prot;
    if (test_state.mmap_errors[i]) {
        errno = test_state.mmap_errors[i];
        return MAP_FAILED;
    }
    return mapped_data;
}

static int mapping_test_close(int fd) {
    assert(fd == test_state.active_fd && fd >= 0);
    test_state.active_fd = -1;
    test_state.close_calls++;
    errno = EIO; /* Successful cleanup must not obscure a mapping failure. */
    return 0;
}

static void check_mapping(int writable, int expected_error) {
    size_t bytes = 0, delta = 0;
    void *mapping = vemb_v16_mapped_region_open(
        test_state.expected_shm ? "/cli-test" : "/dev/obmm_shmdev1",
        test_state.expected_shm ? VEMB_V16_REGION_LOCAL_SHM : VEMB_V16_REGION_UB,
        writable, (uint64_t)sysconf(_SC_PAGESIZE) + 17, 64, &bytes, &delta);
    if (expected_error) {
        assert(mapping == NULL);
        assert(errno == expected_error);
        assert(bytes == 0 && delta == 0);
    } else {
        assert(mapping == mapped_data);
        assert(bytes == 81 && delta == 17);
    }
    assert(test_state.active_fd == -1);
}

int main(void) {
    for (int writable = 0; writable <= 1; writable++) {
        reset_mapping_test(1);
        check_mapping(writable, 0);
        assert(test_state.shm_calls == 1 && test_state.open_calls == 1);
        assert(test_state.open_flags[0] == (writable ? O_RDWR : O_RDONLY));
        assert(test_state.protection == (writable ? PROT_READ | PROT_WRITE : PROT_READ));

        reset_mapping_test(0);
        check_mapping(writable, 0);
        assert(test_state.open_calls == 1 && test_state.shm_calls == 0);
        assert(test_state.open_flags[0] == O_RDWR);
        assert(test_state.protection == (PROT_READ | PROT_WRITE));
    }
    const int permission_errors[] = {EPERM, EACCES};
    for (size_t i = 0; i < 2; i++) {
        int permission = permission_errors[i];
        reset_mapping_test(0);
        test_state.open_errors[0] = permission;
        check_mapping(0, 0);
        assert(test_state.open_calls == 2 && test_state.mmap_calls == 1);
        assert(test_state.open_flags[0] == O_RDWR);
        assert(test_state.open_flags[1] == (O_RDWR | O_SYNC));

        reset_mapping_test(0);
        test_state.mmap_errors[0] = permission;
        check_mapping(0, 0);
        assert(test_state.open_calls == 2 && test_state.mmap_calls == 2);
        assert(test_state.close_calls == 2);
        assert(test_state.open_flags[1] == (O_RDWR | O_SYNC));

        reset_mapping_test(1);
        test_state.open_errors[0] = permission;
        check_mapping(0, permission);
        assert(test_state.open_calls == 1 && test_state.mmap_calls == 0);

        reset_mapping_test(1);
        test_state.mmap_errors[0] = permission;
        check_mapping(0, permission);
        assert(test_state.open_calls == 1 && test_state.mmap_calls == 1);
    }

    reset_mapping_test(0);
    test_state.open_errors[0] = ENOENT;
    check_mapping(0, ENOENT);
    assert(test_state.open_calls == 1 && test_state.mmap_calls == 0);

    reset_mapping_test(0);
    test_state.open_errors[0] = EPERM;
    test_state.open_errors[1] = EACCES;
    check_mapping(0, EACCES);
    assert(test_state.open_calls == 2 && test_state.close_calls == 0);

    reset_mapping_test(0);
    test_state.mmap_errors[0] = ENOMEM;
    check_mapping(0, ENOMEM);
    assert(test_state.open_calls == 1 && test_state.mmap_calls == 1);

    reset_mapping_test(0);
    test_state.mmap_errors[0] = EPERM;
    test_state.open_errors[1] = EACCES;
    check_mapping(0, EACCES);
    assert(test_state.open_calls == 2 && test_state.close_calls == 1);

    reset_mapping_test(0);
    test_state.mmap_errors[0] = EPERM;
    test_state.mmap_errors[1] = EINVAL;
    check_mapping(0, EINVAL);
    assert(test_state.mmap_calls == 2 && test_state.close_calls == 2);
    puts("vemb_v16_cli_mapped_region_ut: PASS");
    return 0;
}
