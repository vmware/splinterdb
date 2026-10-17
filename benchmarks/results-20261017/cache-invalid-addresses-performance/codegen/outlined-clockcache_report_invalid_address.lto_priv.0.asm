
.cache/invalid-address-perf-20261017/builds/outlined/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000000ff5b <clockcache_report_invalid_address.lto_priv.0>:
    ff5b:	push   %rbx
    ff5c:	mov    0x8(%rdi),%rax
    ff60:	xor    %edx,%edx
    ff62:	mov    0xc5067(%rip),%rbx        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
    ff69:	mov    0x8(%rax),%rax
    ff6d:	mov    (%rbx),%r9
    ff70:	mov    0x8(%rax),%r8
    ff74:	mov    %rsi,%rax
    ff77:	div    %r8
    ff7a:	test   %rdx,%rdx
    ff7d:	je     ff8e <clockcache_report_invalid_address.lto_priv.0+0x33>
    ff7f:	mov    %rsi,%rdx
    ff82:	mov    %r8,%rcx
    ff85:	lea    0xa7f7c(%rip),%rsi        # b7f08 <__func__.21+0xa78>
    ff8c:	jmp    ffa4 <clockcache_report_invalid_address.lto_priv.0+0x49>
    ff8e:	mov    0x28(%rdi),%rcx
    ff92:	mov    %rsi,%rdx
    ff95:	lea    0xa7fa4(%rip),%rsi        # b7f40 <__func__.21+0xab0>
    ff9c:	shr    $0x2,%rcx
    ffa0:	imul   %r8,%rcx
    ffa4:	mov    %r9,%rdi
    ffa7:	xor    %eax,%eax
    ffa9:	call   fbf0 <fprintf@plt>
    ffae:	mov    (%rbx),%rdi
    ffb1:	pop    %rbx
    ffb2:	jmp    fe90 <fflush@plt>

Disassembly of section .fini:
