
.cache/invalid-address-perf-20261017/builds/isolated-miss/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000009d1c0 <clockcache_get_internal_async.constprop.0>:
   9d1c0:	mov    0x10(%rdi),%rax
   9d1c4:	cmp    $0x1,%rax
   9d1c8:	je     9d2d0 <clockcache_get_internal_async.constprop.0+0x110>
   9d1ce:	push   %rbp
   9d1cf:	push   %rbx
   9d1d0:	mov    %rdi,%rbx
   9d1d3:	sub    $0x8,%rsp
   9d1d7:	test   %rax,%rax
   9d1da:	je     9d1e0 <clockcache_get_internal_async.constprop.0+0x20>
   9d1dc:	jmp    *%rax
   9d1de:	xchg   %ax,%ax
   9d1e0:	data16 lea 0x38dc0(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   9d1e8:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   9d1f0:	mov    0x30(%rbx),%r9d
   9d1f4:	mov    0x28(%rbx),%rsi
   9d1f8:	mov    (%rax),%rax
   9d1fb:	mov    %rax,0x58(%rbx)
   9d1ff:	lea    -0x1(%r9),%eax
   9d203:	cmp    $0x7,%eax
   9d206:	ja     9d34e <clockcache_get_internal_async.constprop.0+0x18e>
   9d20c:	mov    0x20(%rbx),%rdi
   9d210:	xor    %edx,%edx
   9d212:	mov    0x8(%rdi),%rcx
   9d216:	mov    0x8(%rcx),%rax
   9d21a:	mov    0x8(%rax),%r8
   9d21e:	mov    %rsi,%rax
   9d221:	div    %r8
   9d224:	test   %rdx,%rdx
   9d227:	jne    9d2e0 <clockcache_get_internal_async.constprop.0+0x120>
   9d22d:	mov    0x120(%rcx),%rcx
   9d234:	mov    0x28(%rdi),%rax
   9d238:	mov    %rsi,%rdx
   9d23b:	shr    %cl,%rdx
   9d23e:	shr    $0x2,%rax
   9d242:	cmp    %rax,%rdx
   9d245:	jae    9d320 <clockcache_get_internal_async.constprop.0+0x160>
   9d24b:	mov    0x30(%rdi),%rax
   9d24f:	mov    (%rax,%rdx,4),%eax
   9d252:	mov    $0xffffffff,%edx
   9d257:	mov    %rax,0x60(%rbx)
   9d25b:	cmp    %rdx,%rax
   9d25e:	je     9d298 <clockcache_get_internal_async.constprop.0+0xd8>
   9d260:	lea    0xc(%rip),%rax        # 9d273 <clockcache_get_internal_async.constprop.0+0xb3>
   9d267:	movq   $0x0,0x18(%rbx)
   9d26f:	mov    %rax,0x10(%rbx)
   9d273:	mov    %rbx,%rdi
   9d276:	call   aaa80 <clockcache_get_in_cache_async.constprop.0>
   9d27b:	cmp    $0x1,%eax
   9d27e:	jne    9d2bd <clockcache_get_internal_async.constprop.0+0xfd>
   9d280:	movq   $0x1,0x10(%rbx)
   9d288:	add    $0x8,%rsp
   9d28c:	mov    $0x1,%eax
   9d291:	pop    %rbx
   9d292:	pop    %rbp
   9d293:	ret
   9d294:	nopl   0x0(%rax)
   9d298:	lea    0x11(%rip),%rax        # 9d2b0 <clockcache_get_internal_async.constprop.0+0xf0>
   9d29f:	movq   $0x0,0x18(%rbx)
   9d2a7:	mov    %rax,0x10(%rbx)
   9d2ab:	nopl   0x0(%rax,%rax,1)
   9d2b0:	mov    %rbx,%rdi
   9d2b3:	call   9cc10 <clockcache_get_from_disk_async.constprop.0>
   9d2b8:	cmp    $0x1,%eax
   9d2bb:	je     9d280 <clockcache_get_internal_async.constprop.0+0xc0>
   9d2bd:	add    $0x8,%rsp
   9d2c1:	xor    %eax,%eax
   9d2c3:	pop    %rbx
   9d2c4:	pop    %rbp
   9d2c5:	ret
   9d2c6:	cs nopw 0x0(%rax,%rax,1)
   9d2d0:	mov    $0x1,%eax
   9d2d5:	ret
   9d2d6:	cs nopw 0x0(%rax,%rax,1)
   9d2e0:	mov    0x38ce9(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   9d2e7:	mov    %rsi,%rdx
   9d2ea:	mov    %r8,%rcx
   9d2ed:	xor    %eax,%eax
   9d2ef:	lea    0x1bfba(%rip),%rsi        # b92b0 <__func__.21+0xe20>
   9d2f6:	mov    0x0(%rbp),%rdi
   9d2fa:	call   fbf0 <fprintf@plt>
   9d2ff:	mov    0x0(%rbp),%rdi
   9d303:	call   fe90 <fflush@plt>
   9d308:	mov    0x268b1(%rip),%rax        # c3bc0 <__func__.0.lto_priv.4+0x60>
   9d30f:	movq   $0x1,0x10(%rbx)
   9d317:	mov    %rax,0x50(%rbx)
   9d31b:	jmp    9d288 <clockcache_get_internal_async.constprop.0+0xc8>
   9d320:	mov    0x38ca9(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   9d327:	mov    %r8,%rcx
   9d32a:	mov    %rsi,%rdx
   9d32d:	lea    0x1bfb4(%rip),%rsi        # b92e8 <__func__.21+0xe58>
   9d334:	imul   %rax,%rcx
   9d338:	xor    %eax,%eax
   9d33a:	mov    0x0(%rbp),%rdi
   9d33e:	call   fbf0 <fprintf@plt>
   9d343:	mov    0x0(%rbp),%rdi
   9d347:	call   fe90 <fflush@plt>
   9d34c:	jmp    9d308 <clockcache_get_internal_async.constprop.0+0x148>
   9d34e:	push   %rax
   9d34f:	lea    0x1c2ea(%rip),%r8        # b9640 <__func__.21+0x11b0>
   9d356:	xor    %eax,%eax
   9d358:	lea    0x1c309(%rip),%rcx        # b9668 <__func__.21+0x11d8>
   9d35f:	push   %rsi
   9d360:	lea    0x26539(%rip),%rdx        # c38a0 <__func__.23.lto_priv.1>
   9d367:	mov    $0x1ca,%esi
   9d36c:	lea    0x15264(%rip),%rdi        # b25d7 <_fini+0x137b>
   9d373:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
