
.cache/invalid-address-perf-20261017/builds/hints/release/lib/libsplinterdb.so:     file format elf64-x86-64


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
   aadee:	ja     ab053 <clockcache_get_internal_async.constprop.0+0x2b3>
   aadf4:	mov    0x20(%rbx),%rsi
   aadf8:	xor    %edx,%edx
   aadfa:	mov    0x8(%rsi),%rcx
   aadfe:	mov    0x8(%rcx),%rax
   aae02:	mov    0x8(%rax),%r8
   aae06:	mov    %rbp,%rax
   aae09:	div    %r8
   aae0c:	test   %rdx,%rdx
   aae0f:	jne    aaf68 <clockcache_get_internal_async.constprop.0+0x1c8>
   aae15:	mov    0x120(%rcx),%rcx
   aae1c:	mov    0x28(%rsi),%rax
   aae20:	mov    %rbp,%rdx
   aae23:	shr    %cl,%rdx
   aae26:	shr    $0x2,%rax
   aae2a:	cmp    %rax,%rdx
   aae2d:	jae    aaf20 <clockcache_get_internal_async.constprop.0+0x180>
   aae33:	mov    0x30(%rsi),%rax
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
   aae88:	mov    0x10(%rsi),%r12
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
   aaeaa:	jne    aafde <clockcache_get_internal_async.constprop.0+0x23e>
   aaeb0:	mov    (%r12),%rax
   aaeb4:	mov    %rbp,%r13
   aaeb7:	and    0x20(%rcx),%r13
   aaebb:	je     aaf98 <clockcache_get_internal_async.constprop.0+0x1f8>
   aaec1:	cmp    0x8(%rcx),%rbp
   aaec5:	jae    aaf98 <clockcache_get_internal_async.constprop.0+0x1f8>
   aaecb:	mov    %r13,%rsi
   aaece:	mov    %r12,%rdi
   aaed1:	call   *0x20(%rax)
   aaed4:	test   %eax,%eax
   aaed6:	je     ab014 <clockcache_get_internal_async.constprop.0+0x274>
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
   aaf27:	mov    %r8,%rcx
   aaf2a:	mov    %rbp,%rdx
   aaf2d:	lea    0xe3b4(%rip),%rsi        # b92e8 <__func__.21+0xe58>
   aaf34:	imul   %rax,%rcx
   aaf38:	xor    %eax,%eax
   aaf3a:	mov    (%r12),%rdi
   aaf3e:	call   fbf0 <fprintf@plt>
   aaf43:	mov    (%r12),%rdi
   aaf47:	call   fe90 <fflush@plt>
   aaf4c:	mov    0x18c4d(%rip),%rax        # c3ba0 <__func__.0.lto_priv.4+0x60>
   aaf53:	movq   $0x1,0x10(%rbx)
   aaf5b:	mov    %rax,0x50(%rbx)
   aaf5f:	jmp    aae74 <clockcache_get_internal_async.constprop.0+0xd4>
   aaf64:	nopl   0x0(%rax)
   aaf68:	mov    0x2b061(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aaf6f:	mov    %r8,%rcx
   aaf72:	mov    %rbp,%rdx
   aaf75:	xor    %eax,%eax
   aaf77:	lea    0xe332(%rip),%rsi        # b92b0 <__func__.21+0xe20>
   aaf7e:	mov    (%r12),%rdi
   aaf82:	call   fbf0 <fprintf@plt>
   aaf87:	mov    (%r12),%rdi
   aaf8b:	call   fe90 <fflush@plt>
   aaf90:	jmp    aaf4c <clockcache_get_internal_async.constprop.0+0x1ac>
   aaf92:	nopw   0x0(%rax,%rax,1)
   aaf98:	mov    %r12,%rdi
   aaf9b:	call   *0x58(%rax)
   aaf9e:	mov    0x2b02b(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aafa5:	mov    %rbp,%r9
   aafa8:	mov    %r13,%r8
   aafab:	sub    $0x8,%rsp
   aafaf:	lea    0x188fa(%rip),%rdx        # c38b0 <__func__.22.lto_priv.0>
   aafb6:	mov    $0x17d,%ecx
   aafbb:	mov    (%r12),%rdi
   aafbf:	push   %rax
   aafc0:	lea    0xd599(%rip),%rsi        # b8560 <__func__.21+0xd0>
   aafc7:	xor    %eax,%eax
   aafc9:	call   fbf0 <fprintf@plt>
   aafce:	mov    (%r12),%rdi
   aafd2:	call   fe90 <fflush@plt>
   aafd7:	pop    %rax
   aafd8:	pop    %rdx
   aafd9:	jmp    aaf4c <clockcache_get_internal_async.constprop.0+0x1ac>
   aafde:	mov    0x2afeb(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   aafe5:	mov    %rbp,%r8
   aafe8:	xor    %eax,%eax
   aafea:	mov    $0x164,%ecx
   aafef:	lea    0x188ba(%rip),%rdx        # c38b0 <__func__.22.lto_priv.0>
   aaff6:	lea    0xd4ab(%rip),%rsi        # b84a8 <__func__.21+0x18>
   aaffd:	mov    (%r12),%rdi
   ab001:	call   fbf0 <fprintf@plt>
   ab006:	mov    (%r12),%rdi
   ab00a:	call   fe90 <fflush@plt>
   ab00f:	jmp    aaf4c <clockcache_get_internal_async.constprop.0+0x1ac>
   ab014:	sub    $0x8,%rsp
   ab018:	mov    $0x172,%ecx
   ab01d:	mov    %rbp,%r9
   ab020:	mov    %r13,%r8
   ab023:	mov    0x2afa6(%rip),%r12        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   ab02a:	lea    0xd4c7(%rip),%rsi        # b84f8 <__func__.21+0x68>
   ab031:	lea    0x18878(%rip),%rdx        # c38b0 <__func__.22.lto_priv.0>
   ab038:	mov    (%r12),%rdi
   ab03c:	push   $0x0
   ab03e:	call   fbf0 <fprintf@plt>
   ab043:	mov    (%r12),%rdi
   ab047:	call   fe90 <fflush@plt>
   ab04c:	pop    %rcx
   ab04d:	pop    %rsi
   ab04e:	jmp    aaf4c <clockcache_get_internal_async.constprop.0+0x1ac>
   ab053:	push   %rdi
   ab054:	lea    0xe5e5(%rip),%r8        # b9640 <__func__.21+0x11b0>
   ab05b:	mov    $0x1ca,%esi
   ab060:	xor    %eax,%eax
   ab062:	push   %rbp
   ab063:	lea    0xe5fe(%rip),%rcx        # b9668 <__func__.21+0x11d8>
   ab06a:	lea    0x1880f(%rip),%rdx        # c3880 <__func__.23.lto_priv.1>
   ab071:	lea    0x755f(%rip),%rdi        # b25d7 <_fini+0x14bb>
   ab078:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
