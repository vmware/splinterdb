
.cache/invalid-address-perf-20261017/builds/mask-original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aad60 <clockcache_get_internal_async.constprop.0>:
   aad60:	mov    0x10(%rdi),%rax
   aad64:	cmp    $0x1,%rax
   aad68:	je     aaed0 <clockcache_get_internal_async.constprop.0+0x170>
   aad6e:	push   %r13
   aad70:	push   %r12
   aad72:	push   %rbp
   aad73:	push   %rbx
   aad74:	mov    %rdi,%rbx
   aad77:	sub    $0x8,%rsp
   aad7b:	test   %rax,%rax
   aad7e:	je     aad88 <clockcache_get_internal_async.constprop.0+0x28>
   aad80:	jmp    *%rax
   aad82:	nopw   0x0(%rax,%rax,1)
   aad88:	data16 lea 0x2b218(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   aad90:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   aad98:	mov    0x30(%rbx),%r9d
   aad9c:	mov    0x28(%rbx),%rbp
   aada0:	mov    (%rax),%rax
   aada3:	mov    %rax,0x58(%rbx)
   aada7:	lea    -0x1(%r9),%eax
   aadab:	cmp    $0x7,%eax
   aadae:	ja     ab00b <clockcache_get_internal_async.constprop.0+0x2ab>
   aadb4:	mov    0x20(%rbx),%rax
   aadb8:	mov    0x8(%rax),%rdx
   aadbc:	mov    0x8(%rdx),%rcx
   aadc0:	mov    0x8(%rcx),%rsi
   aadc4:	lea    -0x1(%rsi),%rcx
   aadc8:	test   %rbp,%rcx
   aadcb:	jne    aaee0 <clockcache_get_internal_async.constprop.0+0x180>
   aadd1:	mov    0x120(%rdx),%rcx
   aadd8:	mov    %rbp,%rdx
   aaddb:	shr    %cl,%rdx
   aadde:	mov    %rdx,%rcx
   aade1:	mov    0x28(%rax),%rdx
   aade5:	shr    $0x2,%rdx
   aade9:	cmp    %rdx,%rcx
   aadec:	jae    aaf20 <clockcache_get_internal_async.constprop.0+0x1c0>
   aadf2:	mov    0x30(%rax),%rdx
   aadf6:	mov    (%rdx,%rcx,4),%edx
   aadf9:	mov    $0xffffffff,%ecx
   aadfe:	mov    %rdx,0x60(%rbx)
   aae02:	cmp    %rcx,%rdx
   aae05:	je     aae48 <clockcache_get_internal_async.constprop.0+0xe8>
   aae07:	lea    0xc(%rip),%rax        # aae1a <clockcache_get_internal_async.constprop.0+0xba>
   aae0e:	movq   $0x0,0x18(%rbx)
   aae16:	mov    %rax,0x10(%rbx)
   aae1a:	mov    %rbx,%rdi
   aae1d:	call   aa610 <clockcache_get_in_cache_async.constprop.0>
   aae22:	cmp    $0x1,%eax
   aae25:	jne    aaec1 <clockcache_get_internal_async.constprop.0+0x161>
   aae2b:	movq   $0x1,0x10(%rbx)
   aae33:	add    $0x8,%rsp
   aae37:	mov    $0x1,%eax
   aae3c:	pop    %rbx
   aae3d:	pop    %rbp
   aae3e:	pop    %r12
   aae40:	pop    %r13
   aae42:	ret
   aae43:	nopl   0x0(%rax,%rax,1)
   aae48:	mov    0x10(%rax),%r12
   aae4c:	mov    (%r12),%rax
   aae50:	mov    %r12,%rdi
   aae53:	call   *(%rax)
   aae55:	xor    %edx,%edx
   aae57:	mov    %rax,%rcx
   aae5a:	mov    (%rax),%rax
   aae5d:	mov    0x8(%rax),%r9
   aae61:	mov    %rbp,%rax
   aae64:	div    %r9
   aae67:	test   %rdx,%rdx
   aae6a:	jne    aaf96 <clockcache_get_internal_async.constprop.0+0x236>
   aae70:	mov    (%r12),%rax
   aae74:	mov    %rbp,%r13
   aae77:	and    0x20(%rcx),%r13
   aae7b:	je     aaf50 <clockcache_get_internal_async.constprop.0+0x1f0>
   aae81:	cmp    0x8(%rcx),%rbp
   aae85:	jae    aaf50 <clockcache_get_internal_async.constprop.0+0x1f0>
   aae8b:	mov    %r13,%rsi
   aae8e:	mov    %r12,%rdi
   aae91:	call   *0x20(%rax)
   aae94:	test   %eax,%eax
   aae96:	je     aafcc <clockcache_get_internal_async.constprop.0+0x26c>
   aae9c:	lea    0xd(%rip),%rax        # aaeb0 <clockcache_get_internal_async.constprop.0+0x150>
   aaea3:	movq   $0x0,0x18(%rbx)
   aaeab:	mov    %rax,0x10(%rbx)
   aaeaf:	nop
   aaeb0:	mov    %rbx,%rdi
   aaeb3:	call   aa060 <clockcache_get_from_disk_async.constprop.0>
   aaeb8:	cmp    $0x1,%eax
   aaebb:	je     aae2b <clockcache_get_internal_async.constprop.0+0xcb>
   aaec1:	add    $0x8,%rsp
   aaec5:	xor    %eax,%eax
   aaec7:	pop    %rbx
   aaec8:	pop    %rbp
   aaec9:	pop    %r12
   aaecb:	pop    %r13
   aaecd:	ret
   aaece:	xchg   %ax,%ax
   aaed0:	mov    $0x1,%eax
   aaed5:	ret
   aaed6:	cs nopw 0x0(%rax,%rax,1)
   aaee0:	mov    0x2b0e9(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aaee7:	mov    %rsi,%rcx
   aaeea:	mov    %rbp,%rdx
   aaeed:	xor    %eax,%eax
   aaeef:	lea    0xe3ba(%rip),%rsi        # b92b0 <__func__.21+0xe20>
   aaef6:	mov    (%r12),%rdi
   aaefa:	call   fbf0 <fprintf@plt>
   aaeff:	mov    (%r12),%rdi
   aaf03:	call   fe90 <fflush@plt>
   aaf08:	mov    0x18cd1(%rip),%rax        # c3be0 <__func__.0.lto_priv.4+0x60>
   aaf0f:	movq   $0x1,0x10(%rbx)
   aaf17:	mov    %rax,0x50(%rbx)
   aaf1b:	jmp    aae33 <clockcache_get_internal_async.constprop.0+0xd3>
   aaf20:	imul   %rdx,%rsi
   aaf24:	mov    0x2b0a5(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aaf2b:	mov    %rbp,%rdx
   aaf2e:	xor    %eax,%eax
   aaf30:	mov    (%r12),%rdi
   aaf34:	mov    %rsi,%rcx
   aaf37:	lea    0xe3aa(%rip),%rsi        # b92e8 <__func__.21+0xe58>
   aaf3e:	call   fbf0 <fprintf@plt>
   aaf43:	mov    (%r12),%rdi
   aaf47:	call   fe90 <fflush@plt>
   aaf4c:	jmp    aaf08 <clockcache_get_internal_async.constprop.0+0x1a8>
   aaf4e:	xchg   %ax,%ax
   aaf50:	mov    %r12,%rdi
   aaf53:	call   *0x58(%rax)
   aaf56:	mov    0x2b073(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aaf5d:	mov    %rbp,%r9
   aaf60:	mov    %r13,%r8
   aaf63:	sub    $0x8,%rsp
   aaf67:	lea    0x18982(%rip),%rdx        # c38f0 <__func__.22.lto_priv.0>
   aaf6e:	mov    $0x17d,%ecx
   aaf73:	mov    (%r12),%rdi
   aaf77:	push   %rax
   aaf78:	lea    0xd5e1(%rip),%rsi        # b8560 <__func__.21+0xd0>
   aaf7f:	xor    %eax,%eax
   aaf81:	call   fbf0 <fprintf@plt>
   aaf86:	mov    (%r12),%rdi
   aaf8a:	call   fe90 <fflush@plt>
   aaf8f:	pop    %rax
   aaf90:	pop    %rdx
   aaf91:	jmp    aaf08 <clockcache_get_internal_async.constprop.0+0x1a8>
   aaf96:	mov    0x2b033(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aaf9d:	mov    %rbp,%r8
   aafa0:	xor    %eax,%eax
   aafa2:	mov    $0x164,%ecx
   aafa7:	lea    0x18942(%rip),%rdx        # c38f0 <__func__.22.lto_priv.0>
   aafae:	lea    0xd4f3(%rip),%rsi        # b84a8 <__func__.21+0x18>
   aafb5:	mov    (%r12),%rdi
   aafb9:	call   fbf0 <fprintf@plt>
   aafbe:	mov    (%r12),%rdi
   aafc2:	call   fe90 <fflush@plt>
   aafc7:	jmp    aaf08 <clockcache_get_internal_async.constprop.0+0x1a8>
   aafcc:	sub    $0x8,%rsp
   aafd0:	mov    $0x172,%ecx
   aafd5:	mov    %rbp,%r9
   aafd8:	mov    %r13,%r8
   aafdb:	mov    0x2afee(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aafe2:	lea    0xd50f(%rip),%rsi        # b84f8 <__func__.21+0x68>
   aafe9:	lea    0x18900(%rip),%rdx        # c38f0 <__func__.22.lto_priv.0>
   aaff0:	mov    (%r12),%rdi
   aaff4:	push   $0x0
   aaff6:	call   fbf0 <fprintf@plt>
   aaffb:	mov    (%r12),%rdi
   aafff:	call   fe90 <fflush@plt>
   ab004:	pop    %rcx
   ab005:	pop    %rsi
   ab006:	jmp    aaf08 <clockcache_get_internal_async.constprop.0+0x1a8>
   ab00b:	push   %rdi
   ab00c:	lea    0xe655(%rip),%r8        # b9668 <__func__.21+0x11d8>
   ab013:	mov    $0x1ca,%esi
   ab018:	xor    %eax,%eax
   ab01a:	push   %rbp
   ab01b:	lea    0xe66e(%rip),%rcx        # b9690 <__func__.21+0x1200>
   ab022:	lea    0x18897(%rip),%rdx        # c38c0 <__func__.23.lto_priv.1>
   ab029:	lea    0x75a7(%rip),%rdi        # b25d7 <_fini+0x14fb>
   ab030:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
