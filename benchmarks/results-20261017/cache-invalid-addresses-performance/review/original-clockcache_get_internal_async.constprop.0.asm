
.cache/invalid-address-perf-20261017/builds/original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aada0 <clockcache_get_internal_async.constprop.0>:
   aada0:	mov    0x10(%rdi),%rax
   aada4:	cmp    $0x1,%rax
   aada8:	je     aaf10 <clockcache_get_internal_async.constprop.0+0x170>
   aadae:	push   %r13
   aadb0:	push   %r12
   aadb2:	push   %rbp
   aadb3:	push   %rbx
   aadb4:	mov    %rdi,%rbx
   aadb7:	sub    $0x8,%rsp
   aadbb:	test   %rax,%rax
   aadbe:	je     aadc8 <clockcache_get_internal_async.constprop.0+0x28>
   aadc0:	jmp    *%rax
   aadc2:	nopw   0x0(%rax,%rax,1)
   aadc8:	data16 lea 0x2b1d8(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   aadd0:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   aadd8:	mov    0x30(%rbx),%r9d
   aaddc:	mov    0x28(%rbx),%rbp
   aade0:	mov    (%rax),%rax
   aade3:	mov    %rax,0x58(%rbx)
   aade7:	lea    -0x1(%r9),%eax
   aadeb:	cmp    $0x7,%eax
   aadee:	ja     ab04b <clockcache_get_internal_async.constprop.0+0x2ab>
   aadf4:	mov    0x20(%rbx),%rdi
   aadf8:	xor    %edx,%edx
   aadfa:	mov    0x8(%rdi),%rcx
   aadfe:	mov    0x8(%rcx),%rax
   aae02:	mov    0x8(%rax),%rsi
   aae06:	mov    %rbp,%rax
   aae09:	div    %rsi
   aae0c:	test   %rdx,%rdx
   aae0f:	jne    aaf20 <clockcache_get_internal_async.constprop.0+0x180>
   aae15:	mov    0x120(%rcx),%rcx
   aae1c:	mov    0x28(%rdi),%rax
   aae20:	mov    %rbp,%rdx
   aae23:	shr    %cl,%rdx
   aae26:	shr    $0x2,%rax
   aae2a:	cmp    %rax,%rdx
   aae2d:	jae    aaf60 <clockcache_get_internal_async.constprop.0+0x1c0>
   aae33:	mov    0x30(%rdi),%rax
   aae37:	mov    (%rax,%rdx,4),%eax
   aae3a:	mov    $0xffffffff,%edx
   aae3f:	mov    %rax,0x60(%rbx)
   aae43:	cmp    %rdx,%rax
   aae46:	je     aae88 <clockcache_get_internal_async.constprop.0+0xe8>
   aae48:	lea    0xc(%rip),%rax        # aae5b <clockcache_get_internal_async.constprop.0+0xbb>
   aae4f:	movq   $0x0,0x18(%rbx)
   aae57:	mov    %rax,0x10(%rbx)
   aae5b:	mov    %rbx,%rdi
   aae5e:	call   aa650 <clockcache_get_in_cache_async.constprop.0>
   aae63:	cmp    $0x1,%eax
   aae66:	jne    aaf01 <clockcache_get_internal_async.constprop.0+0x161>
   aae6c:	movq   $0x1,0x10(%rbx)
   aae74:	add    $0x8,%rsp
   aae78:	mov    $0x1,%eax
   aae7d:	pop    %rbx
   aae7e:	pop    %rbp
   aae7f:	pop    %r12
   aae81:	pop    %r13
   aae83:	ret
   aae84:	nopl   0x0(%rax)
   aae88:	mov    0x10(%rdi),%r12
   aae8c:	mov    (%r12),%rax
   aae90:	mov    %r12,%rdi
   aae93:	call   *(%rax)
   aae95:	xor    %edx,%edx
   aae97:	mov    %rax,%rcx
   aae9a:	mov    (%rax),%rax
   aae9d:	mov    0x8(%rax),%r9
   aaea1:	mov    %rbp,%rax
   aaea4:	div    %r9
   aaea7:	test   %rdx,%rdx
   aaeaa:	jne    aafd6 <clockcache_get_internal_async.constprop.0+0x236>
   aaeb0:	mov    (%r12),%rax
   aaeb4:	mov    %rbp,%r13
   aaeb7:	and    0x20(%rcx),%r13
   aaebb:	je     aaf90 <clockcache_get_internal_async.constprop.0+0x1f0>
   aaec1:	cmp    0x8(%rcx),%rbp
   aaec5:	jae    aaf90 <clockcache_get_internal_async.constprop.0+0x1f0>
   aaecb:	mov    %r13,%rsi
   aaece:	mov    %r12,%rdi
   aaed1:	call   *0x20(%rax)
   aaed4:	test   %eax,%eax
   aaed6:	je     ab00c <clockcache_get_internal_async.constprop.0+0x26c>
   aaedc:	lea    0xd(%rip),%rax        # aaef0 <clockcache_get_internal_async.constprop.0+0x150>
   aaee3:	movq   $0x0,0x18(%rbx)
   aaeeb:	mov    %rax,0x10(%rbx)
   aaeef:	nop
   aaef0:	mov    %rbx,%rdi
   aaef3:	call   aa0a0 <clockcache_get_from_disk_async.constprop.0>
   aaef8:	cmp    $0x1,%eax
   aaefb:	je     aae6c <clockcache_get_internal_async.constprop.0+0xcc>
   aaf01:	add    $0x8,%rsp
   aaf05:	xor    %eax,%eax
   aaf07:	pop    %rbx
   aaf08:	pop    %rbp
   aaf09:	pop    %r12
   aaf0b:	pop    %r13
   aaf0d:	ret
   aaf0e:	xchg   %ax,%ax
   aaf10:	mov    $0x1,%eax
   aaf15:	ret
   aaf16:	cs nopw 0x0(%rax,%rax,1)
   aaf20:	mov    0x2b0a9(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aaf27:	mov    %rsi,%rcx
   aaf2a:	mov    %rbp,%rdx
   aaf2d:	xor    %eax,%eax
   aaf2f:	lea    0xe37a(%rip),%rsi        # b92b0 <__func__.21+0xe20>
   aaf36:	mov    (%r12),%rdi
   aaf3a:	call   fbf0 <fprintf@plt>
   aaf3f:	mov    (%r12),%rdi
   aaf43:	call   fe90 <fflush@plt>
   aaf48:	mov    0x18c51(%rip),%rax        # c3ba0 <__func__.0.lto_priv.4+0x60>
   aaf4f:	movq   $0x1,0x10(%rbx)
   aaf57:	mov    %rax,0x50(%rbx)
   aaf5b:	jmp    aae74 <clockcache_get_internal_async.constprop.0+0xd4>
   aaf60:	imul   %rax,%rsi
   aaf64:	mov    0x2b065(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aaf6b:	mov    %rbp,%rdx
   aaf6e:	xor    %eax,%eax
   aaf70:	mov    (%r12),%rdi
   aaf74:	mov    %rsi,%rcx
   aaf77:	lea    0xe36a(%rip),%rsi        # b92e8 <__func__.21+0xe58>
   aaf7e:	call   fbf0 <fprintf@plt>
   aaf83:	mov    (%r12),%rdi
   aaf87:	call   fe90 <fflush@plt>
   aaf8c:	jmp    aaf48 <clockcache_get_internal_async.constprop.0+0x1a8>
   aaf8e:	xchg   %ax,%ax
   aaf90:	mov    %r12,%rdi
   aaf93:	call   *0x58(%rax)
   aaf96:	mov    0x2b033(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aaf9d:	mov    %rbp,%r9
   aafa0:	mov    %r13,%r8
   aafa3:	sub    $0x8,%rsp
   aafa7:	lea    0x18902(%rip),%rdx        # c38b0 <__func__.22.lto_priv.0>
   aafae:	mov    $0x17d,%ecx
   aafb3:	mov    (%r12),%rdi
   aafb7:	push   %rax
   aafb8:	lea    0xd5a1(%rip),%rsi        # b8560 <__func__.21+0xd0>
   aafbf:	xor    %eax,%eax
   aafc1:	call   fbf0 <fprintf@plt>
   aafc6:	mov    (%r12),%rdi
   aafca:	call   fe90 <fflush@plt>
   aafcf:	pop    %rax
   aafd0:	pop    %rdx
   aafd1:	jmp    aaf48 <clockcache_get_internal_async.constprop.0+0x1a8>
   aafd6:	mov    0x2aff3(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aafdd:	mov    %rbp,%r8
   aafe0:	xor    %eax,%eax
   aafe2:	mov    $0x164,%ecx
   aafe7:	lea    0x188c2(%rip),%rdx        # c38b0 <__func__.22.lto_priv.0>
   aafee:	lea    0xd4b3(%rip),%rsi        # b84a8 <__func__.21+0x18>
   aaff5:	mov    (%r12),%rdi
   aaff9:	call   fbf0 <fprintf@plt>
   aaffe:	mov    (%r12),%rdi
   ab002:	call   fe90 <fflush@plt>
   ab007:	jmp    aaf48 <clockcache_get_internal_async.constprop.0+0x1a8>
   ab00c:	sub    $0x8,%rsp
   ab010:	mov    $0x172,%ecx
   ab015:	mov    %rbp,%r9
   ab018:	mov    %r13,%r8
   ab01b:	mov    0x2afae(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   ab022:	lea    0xd4cf(%rip),%rsi        # b84f8 <__func__.21+0x68>
   ab029:	lea    0x18880(%rip),%rdx        # c38b0 <__func__.22.lto_priv.0>
   ab030:	mov    (%r12),%rdi
   ab034:	push   $0x0
   ab036:	call   fbf0 <fprintf@plt>
   ab03b:	mov    (%r12),%rdi
   ab03f:	call   fe90 <fflush@plt>
   ab044:	pop    %rcx
   ab045:	pop    %rsi
   ab046:	jmp    aaf48 <clockcache_get_internal_async.constprop.0+0x1a8>
   ab04b:	push   %rdi
   ab04c:	lea    0xe5ed(%rip),%r8        # b9640 <__func__.21+0x11b0>
   ab053:	mov    $0x1ca,%esi
   ab058:	xor    %eax,%eax
   ab05a:	push   %rbp
   ab05b:	lea    0xe606(%rip),%rcx        # b9668 <__func__.21+0x11d8>
   ab062:	lea    0x18817(%rip),%rdx        # c3880 <__func__.23.lto_priv.1>
   ab069:	lea    0x7567(%rip),%rdi        # b25d7 <_fini+0x14bb>
   ab070:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
