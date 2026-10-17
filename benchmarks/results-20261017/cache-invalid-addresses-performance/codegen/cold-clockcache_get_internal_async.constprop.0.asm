
.cache/invalid-address-perf-20261017/builds/cold/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aaaa0 <clockcache_get_internal_async.constprop.0>:
   aaaa0:	mov    0x10(%rdi),%rax
   aaaa4:	cmp    $0x1,%rax
   aaaa8:	je     aac10 <clockcache_get_internal_async.constprop.0+0x170>
   aaaae:	push   %r13
   aaab0:	push   %r12
   aaab2:	push   %rbp
   aaab3:	push   %rbx
   aaab4:	mov    %rdi,%rbx
   aaab7:	sub    $0x8,%rsp
   aaabb:	test   %rax,%rax
   aaabe:	je     aaac8 <clockcache_get_internal_async.constprop.0+0x28>
   aaac0:	jmp    *%rax
   aaac2:	nopw   0x0(%rax,%rax,1)
   aaac8:	data16 lea 0x2a4d8(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   aaad0:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   aaad8:	mov    0x30(%rbx),%r9d
   aaadc:	mov    0x28(%rbx),%rbp
   aaae0:	mov    (%rax),%rax
   aaae3:	mov    %rax,0x58(%rbx)
   aaae7:	lea    -0x1(%r9),%eax
   aaaeb:	cmp    $0x7,%eax
   aaaee:	ja     aaceb <clockcache_get_internal_async.constprop.0+0x24b>
   aaaf4:	mov    0x20(%rbx),%rdi
   aaaf8:	mov    %rbp,%rsi
   aaafb:	xor    %edx,%edx
   aaafd:	mov    0x8(%rdi),%rax
   aab01:	mov    0x120(%rax),%rcx
   aab08:	shr    %cl,%rsi
   aab0b:	mov    %rsi,%rcx
   aab0e:	mov    0x8(%rax),%rsi
   aab12:	mov    %rbp,%rax
   aab15:	divq   0x8(%rsi)
   aab19:	test   %rdx,%rdx
   aab1c:	jne    1057b <clockcache_get_internal_async.constprop.0.cold>
   aab22:	mov    0x28(%rdi),%rax
   aab26:	shr    $0x2,%rax
   aab2a:	cmp    %rax,%rcx
   aab2d:	jae    1057b <clockcache_get_internal_async.constprop.0.cold>
   aab33:	mov    0x30(%rdi),%rax
   aab37:	mov    $0xffffffff,%edx
   aab3c:	mov    (%rax,%rcx,4),%eax
   aab3f:	mov    %rax,0x60(%rbx)
   aab43:	cmp    %rdx,%rax
   aab46:	je     aab88 <clockcache_get_internal_async.constprop.0+0xe8>
   aab48:	lea    0xc(%rip),%rax        # aab5b <clockcache_get_internal_async.constprop.0+0xbb>
   aab4f:	movq   $0x0,0x18(%rbx)
   aab57:	mov    %rax,0x10(%rbx)
   aab5b:	mov    %rbx,%rdi
   aab5e:	call   aa350 <clockcache_get_in_cache_async.constprop.0>
   aab63:	cmp    $0x1,%eax
   aab66:	jne    aac01 <clockcache_get_internal_async.constprop.0+0x161>
   aab6c:	movq   $0x1,0x10(%rbx)
   aab74:	add    $0x8,%rsp
   aab78:	mov    $0x1,%eax
   aab7d:	pop    %rbx
   aab7e:	pop    %rbp
   aab7f:	pop    %r12
   aab81:	pop    %r13
   aab83:	ret
   aab84:	nopl   0x0(%rax)
   aab88:	mov    0x10(%rdi),%r12
   aab8c:	mov    (%r12),%rax
   aab90:	mov    %r12,%rdi
   aab93:	call   *(%rax)
   aab95:	xor    %edx,%edx
   aab97:	mov    %rax,%rcx
   aab9a:	mov    (%rax),%rax
   aab9d:	mov    0x8(%rax),%r9
   aaba1:	mov    %rbp,%rax
   aaba4:	div    %r9
   aaba7:	test   %rdx,%rdx
   aabaa:	jne    aac79 <clockcache_get_internal_async.constprop.0+0x1d9>
   aabb0:	mov    (%r12),%rax
   aabb4:	mov    %rbp,%r13
   aabb7:	and    0x20(%rcx),%r13
   aabbb:	je     aac20 <clockcache_get_internal_async.constprop.0+0x180>
   aabbd:	cmp    0x8(%rcx),%rbp
   aabc1:	jae    aac20 <clockcache_get_internal_async.constprop.0+0x180>
   aabc3:	mov    %r13,%rsi
   aabc6:	mov    %r12,%rdi
   aabc9:	call   *0x20(%rax)
   aabcc:	test   %eax,%eax
   aabce:	je     aacac <clockcache_get_internal_async.constprop.0+0x20c>
   aabd4:	lea    0x15(%rip),%rax        # aabf0 <clockcache_get_internal_async.constprop.0+0x150>
   aabdb:	movq   $0x0,0x18(%rbx)
   aabe3:	mov    %rax,0x10(%rbx)
   aabe7:	nopw   0x0(%rax,%rax,1)
   aabf0:	mov    %rbx,%rdi
   aabf3:	call   a9da0 <clockcache_get_from_disk_async.constprop.0>
   aabf8:	cmp    $0x1,%eax
   aabfb:	je     aab6c <clockcache_get_internal_async.constprop.0+0xcc>
   aac01:	add    $0x8,%rsp
   aac05:	xor    %eax,%eax
   aac07:	pop    %rbx
   aac08:	pop    %rbp
   aac09:	pop    %r12
   aac0b:	pop    %r13
   aac0d:	ret
   aac0e:	xchg   %ax,%ax
   aac10:	mov    $0x1,%eax
   aac15:	ret
   aac16:	cs nopw 0x0(%rax,%rax,1)
   aac20:	mov    %r12,%rdi
   aac23:	call   *0x58(%rax)
   aac26:	mov    0x2a3a3(%rip),%r12        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   aac2d:	mov    %rbp,%r9
   aac30:	mov    %r13,%r8
   aac33:	sub    $0x8,%rsp
   aac37:	lea    0x17c72(%rip),%rdx        # c28b0 <__func__.22.lto_priv.0>
   aac3e:	mov    $0x17d,%ecx
   aac43:	mov    (%r12),%rdi
   aac47:	push   %rax
   aac48:	lea    0xc911(%rip),%rsi        # b7560 <__func__.21+0xd0>
   aac4f:	xor    %eax,%eax
   aac51:	call   fbf0 <fprintf@plt>
   aac56:	mov    (%r12),%rdi
   aac5a:	call   fe90 <fflush@plt>
   aac5f:	pop    %rax
   aac60:	pop    %rdx
   aac61:	mov    0x17f38(%rip),%rax        # c2ba0 <__func__.0.lto_priv.4+0x60>
   aac68:	movq   $0x1,0x10(%rbx)
   aac70:	mov    %rax,0x50(%rbx)
   aac74:	jmp    aab74 <clockcache_get_internal_async.constprop.0+0xd4>
   aac79:	mov    0x2a350(%rip),%r12        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   aac80:	mov    %rbp,%r8
   aac83:	xor    %eax,%eax
   aac85:	mov    $0x164,%ecx
   aac8a:	lea    0x17c1f(%rip),%rdx        # c28b0 <__func__.22.lto_priv.0>
   aac91:	lea    0xc810(%rip),%rsi        # b74a8 <__func__.21+0x18>
   aac98:	mov    (%r12),%rdi
   aac9c:	call   fbf0 <fprintf@plt>
   aaca1:	mov    (%r12),%rdi
   aaca5:	call   fe90 <fflush@plt>
   aacaa:	jmp    aac61 <clockcache_get_internal_async.constprop.0+0x1c1>
   aacac:	sub    $0x8,%rsp
   aacb0:	mov    $0x172,%ecx
   aacb5:	mov    %rbp,%r9
   aacb8:	mov    %r13,%r8
   aacbb:	mov    0x2a30e(%rip),%r12        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   aacc2:	lea    0xc82f(%rip),%rsi        # b74f8 <__func__.21+0x68>
   aacc9:	lea    0x17be0(%rip),%rdx        # c28b0 <__func__.22.lto_priv.0>
   aacd0:	mov    (%r12),%rdi
   aacd4:	push   $0x0
   aacd6:	call   fbf0 <fprintf@plt>
   aacdb:	mov    (%r12),%rdi
   aacdf:	call   fe90 <fflush@plt>
   aace4:	pop    %rcx
   aace5:	pop    %rsi
   aace6:	jmp    aac61 <clockcache_get_internal_async.constprop.0+0x1c1>
   aaceb:	push   %rdi
   aacec:	lea    0xd94d(%rip),%r8        # b8640 <__func__.21+0x11b0>
   aacf3:	mov    $0x1d2,%esi
   aacf8:	xor    %eax,%eax
   aacfa:	push   %rbp
   aacfb:	lea    0xd966(%rip),%rcx        # b8668 <__func__.21+0x11d8>
   aad02:	lea    0x17b77(%rip),%rdx        # c2880 <__func__.23.lto_priv.1>
   aad09:	lea    0x68c7(%rip),%rdi        # b15d7 <_fini+0x8fb>
   aad10:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
