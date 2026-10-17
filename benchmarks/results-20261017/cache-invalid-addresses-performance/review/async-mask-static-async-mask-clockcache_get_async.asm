
.cache/invalid-address-perf-20261017/builds/async-mask/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000050350 <clockcache_get_async>:
   50350:	mov    0x8(%rdi),%rax
   50354:	cmp    $0x1,%rax
   50358:	je     503b8 <clockcache_get_async+0x68>
   5035a:	push   %rbx
   5035b:	mov    %rdi,%rbx
   5035e:	test   %rax,%rax
   50361:	je     50390 <clockcache_get_async+0x40>
   50363:	mov    %rbx,%rdi
   50366:	call   9cd00 <clockcache_get_internal_async.constprop.0>
   5036b:	mov    %eax,%edx
   5036d:	xor    %eax,%eax
   5036f:	cmp    $0x1,%edx
   50372:	jne    50388 <clockcache_get_async+0x38>
   50374:	mov    0x54(%rbx),%eax
   50377:	test   %eax,%eax
   50379:	je     503a0 <clockcache_get_async+0x50>
   5037b:	movq   $0x1,0x8(%rbx)
   50383:	mov    $0x1,%eax
   50388:	pop    %rbx
   50389:	ret
   5038a:	nopw   0x0(%rax,%rax,1)
   50390:	movq   $0x0,0x50(%rdi)
   50398:	movq   $0x0,0x48(%rdi)
   503a0:	lea    -0x44(%rip),%rax        # 50363 <clockcache_get_async+0x13>
   503a7:	movq   $0x0,0x10(%rbx)
   503af:	mov    %rax,0x8(%rbx)
   503b3:	jmp    50363 <clockcache_get_async+0x13>
   503b5:	nopl   (%rax)
   503b8:	mov    $0x1,%eax
   503bd:	ret

Disassembly of section .fini:
