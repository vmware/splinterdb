
.cache/invalid-address-perf-20261017/builds/original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000050050 <clockcache_get_async>:
   50050:	mov    0x8(%rdi),%rax
   50054:	cmp    $0x1,%rax
   50058:	je     500b8 <clockcache_get_async+0x68>
   5005a:	push   %rbx
   5005b:	mov    %rdi,%rbx
   5005e:	test   %rax,%rax
   50061:	je     50090 <clockcache_get_async+0x40>
   50063:	mov    %rbx,%rdi
   50066:	call   aada0 <clockcache_get_internal_async.constprop.0>
   5006b:	mov    %eax,%edx
   5006d:	xor    %eax,%eax
   5006f:	cmp    $0x1,%edx
   50072:	jne    50088 <clockcache_get_async+0x38>
   50074:	mov    0x54(%rbx),%eax
   50077:	test   %eax,%eax
   50079:	je     500a0 <clockcache_get_async+0x50>
   5007b:	movq   $0x1,0x8(%rbx)
   50083:	mov    $0x1,%eax
   50088:	pop    %rbx
   50089:	ret
   5008a:	nopw   0x0(%rax,%rax,1)
   50090:	movq   $0x0,0x50(%rdi)
   50098:	movq   $0x0,0x48(%rdi)
   500a0:	lea    -0x44(%rip),%rax        # 50063 <clockcache_get_async+0x13>
   500a7:	movq   $0x0,0x10(%rbx)
   500af:	mov    %rax,0x8(%rbx)
   500b3:	jmp    50063 <clockcache_get_async+0x13>
   500b5:	nopl   (%rax)
   500b8:	mov    $0x1,%eax
   500bd:	ret

Disassembly of section .fini:
