
.cache/invalid-address-perf-20261017/builds/candidate/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000053b60 <clockcache_get>:
   53b60:	push   %r15
   53b62:	lea    -0x1(%rcx),%eax
   53b65:	push   %r14
   53b67:	push   %r13
   53b69:	push   %r12
   53b6b:	push   %rbp
   53b6c:	push   %rbx
   53b6d:	mov    %rsi,%rbx
   53b70:	sub    $0x48,%rsp
   53b74:	mov    %edx,0x8(%rsp)
   53b78:	mov    %ecx,0x1c(%rsp)
   53b7c:	mov    %r8,0x20(%rsp)
   53b81:	cmp    $0x7,%eax
   53b84:	ja     540a8 <clockcache_get+0x548>
   53b8a:	mov    0x8(%rdi),%rcx
   53b8e:	mov    %rdi,%r14
   53b91:	mov    %rsi,%rax
   53b94:	xor    %edx,%edx
   53b96:	mov    0x8(%rcx),%rdi
   53b9a:	divq   0x8(%rdi)
   53b9e:	test   %rdx,%rdx
   53ba1:	jne    1015d <clockcache_get.cold>
   53ba7:	movabs $0xcccccccccccccccd,%r12
   53bb1:	mov    %r14,%r15
   53bb4:	data16 cs nopw 0x0(%rax,%rax,1)
   53bbf:	nop
   53bc0:	mov    0x120(%rcx),%rcx
   53bc7:	mov    0x28(%r15),%rax
   53bcb:	mov    %rbx,%rdx
   53bce:	shr    %cl,%rdx
   53bd1:	shr    $0x2,%rax
   53bd5:	cmp    %rax,%rdx
   53bd8:	jae    540da <clockcache_get+0x57a>
   53bde:	mov    0x30(%r15),%rax
   53be2:	mov    (%rax,%rdx,4),%ebp
   53be5:	cmp    $0xffffffff,%ebp
   53be8:	je     53d88 <clockcache_get+0x228>
   53bee:	data16 lea 0x813b2(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53bf6:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53bfe:	mov    0x8(%rsp),%r9d
   53c03:	mov    %ebp,%esi
   53c05:	mov    %r15,%rdi
   53c08:	mov    (%rax),%rax
   53c0b:	mov    %rax,0x10(%rsp)
   53c10:	test   %r9d,%r9d
   53c13:	je     53d50 <clockcache_get+0x1f0>
   53c19:	call   aa2e0 <clockcache_try_get_read.constprop.0>
   53c1e:	cmp    $0x1,%eax
   53c21:	jne    53c6d <clockcache_get+0x10d>
   53c23:	mov    $0x1,%r13d
   53c29:	mov    %r13,%rax
   53c2c:	mul    %r12
   53c2f:	xor    %eax,%eax
   53c31:	shr    $0x2,%rdx
   53c35:	mov    %rdx,%r14
   53c38:	nopl   0x0(%rax,%rax,1)
   53c40:	pause
   53c42:	mov    %rax,%rcx
   53c45:	add    $0x1,%rax
   53c49:	cmp    %r14,%rcx
   53c4c:	jne    53c40 <clockcache_get+0xe0>
   53c4e:	cmp    $0x400,%r13
   53c55:	ja     53d20 <clockcache_get+0x1c0>
   53c5b:	mov    %ebp,%esi
   53c5d:	mov    %r15,%rdi
   53c60:	add    %r13,%r13
   53c63:	call   aa2e0 <clockcache_try_get_read.constprop.0>
   53c68:	cmp    $0x1,%eax
   53c6b:	je     53c29 <clockcache_get+0xc9>
   53c6d:	test   %eax,%eax
   53c6f:	jne    53d63 <clockcache_get+0x203>
   53c75:	mov    %ebp,%eax
   53c77:	mov    0x48(%r15),%rcx
   53c7b:	mov    %rax,%rdx
   53c7e:	shl    $0x6,%rdx
   53c82:	mov    0x8(%rcx,%rdx,1),%rsi
   53c87:	cmp    %rsi,%rbx
   53c8a:	jne    53f0e <clockcache_get+0x3ae>
   53c90:	mov    %r15,%r14
   53c93:	mov    %rax,%rbx
   53c96:	mov    %rdx,%r15
   53c99:	jmp    53cac <clockcache_get+0x14c>
   53c9b:	nopl   0x0(%rax,%rax,1)
   53ca0:	mov    %r14,%rdi
   53ca3:	call   ea40 <clockcache_wait@plt>
   53ca8:	mov    0x48(%r14),%rcx
   53cac:	mov    0x10(%rcx,%r15,1),%eax
   53cb1:	test   $0x10,%al
   53cb3:	jne    53ca0 <clockcache_get+0x140>
   53cb5:	mov    0x48(%r14),%rdx
   53cb9:	mov    %rbx,%rax
   53cbc:	add    %r15,%rdx
   53cbf:	mov    0x14(%rdx),%r13d
   53cc3:	test   %r13d,%r13d
   53cc6:	jne    54029 <clockcache_get+0x4c9>
   53ccc:	mov    0x20(%rdx),%eax
   53ccf:	cmp    %eax,0x1c(%rsp)
   53cd3:	jne    54071 <clockcache_get+0x511>
   53cd9:	mov    0x8(%r14),%rax
   53cdd:	mov    0x18(%rax),%edi
   53ce0:	test   %edi,%edi
   53ce2:	jne    53ee8 <clockcache_get+0x388>
   53ce8:	mov    0x20(%rsp),%rbx
   53ced:	xor    %r13d,%r13d
   53cf0:	mov    %rdx,(%rbx)
   53cf3:	data16 lea 0x812ad(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53cfb:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53d03:	mov    (%rax),%rax
   53d06:	mov    %rax,0x8(%rbx)
   53d0a:	add    $0x48,%rsp
   53d0e:	mov    %r13d,%eax
   53d11:	pop    %rbx
   53d12:	pop    %rbp
   53d13:	pop    %r12
   53d15:	pop    %r13
   53d17:	pop    %r14
   53d19:	pop    %r15
   53d1b:	ret
   53d1c:	nopl   0x0(%rax)
   53d20:	mov    %ebp,%esi
   53d22:	mov    %r15,%rdi
   53d25:	call   aa2e0 <clockcache_try_get_read.constprop.0>
   53d2a:	cmp    $0x1,%eax
   53d2d:	jne    53c6d <clockcache_get+0x10d>
   53d33:	xor    %eax,%eax
   53d35:	data16 cs nopw 0x0(%rax,%rax,1)
   53d40:	pause
   53d42:	mov    %rax,%rdx
   53d45:	add    $0x1,%rax
   53d49:	cmp    %r14,%rdx
   53d4c:	jne    53d40 <clockcache_get+0x1e0>
   53d4e:	jmp    53d20 <clockcache_get+0x1c0>
   53d50:	call   aa2e0 <clockcache_try_get_read.constprop.0>
   53d55:	cmp    $0x1,%eax
   53d58:	je     53d9f <clockcache_get+0x23f>
   53d5a:	cmp    $0x2,%eax
   53d5d:	jne    53c75 <clockcache_get+0x115>
   53d63:	mov    0x8(%r15),%rcx
   53d67:	mov    %rbx,%rax
   53d6a:	xor    %edx,%edx
   53d6c:	mov    0x8(%rcx),%rsi
   53d70:	divq   0x8(%rsi)
   53d74:	test   %rdx,%rdx
   53d77:	je     53bc0 <clockcache_get+0x60>
   53d7d:	mov    %r15,%r14
   53d80:	jmp    1015d <clockcache_get.cold>
   53d85:	nopl   (%rax)
   53d88:	mov    %rbx,%rsi
   53d8b:	mov    %r15,%rdi
   53d8e:	call   50080 <clockcache_page_valid_for_load.lto_priv.0>
   53d93:	test   %eax,%eax
   53d95:	je     53db0 <clockcache_get+0x250>
   53d97:	mov    0x8(%rsp),%esi
   53d9b:	test   %esi,%esi
   53d9d:	jne    53dbb <clockcache_get+0x25b>
   53d9f:	mov    $0xb,%r13d
   53da5:	jmp    53d0a <clockcache_get+0x1aa>
   53daa:	nopw   0x0(%rax,%rax,1)
   53db0:	mov    $0x16,%r13d
   53db6:	jmp    53d0a <clockcache_get+0x1aa>
   53dbb:	data16 lea 0x811e5(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53dc3:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53dcb:	mov    0x1c(%rsp),%edx
   53dcf:	mov    %rbx,%rsi
   53dd2:	mov    %r15,%rdi
   53dd5:	mov    (%rax),%r13
   53dd8:	mov    0x8(%r15),%rax
   53ddc:	mov    0x8(%rax),%rax
   53de0:	mov    0x8(%rax),%r14
   53de4:	call   53800 <clockcache_acquire_entry_for_load.lto_priv.0>
   53de9:	mov    %rax,%rbp
   53dec:	mov    $0xffffffff,%eax
   53df1:	cmp    %rax,%rbp
   53df4:	je     53d63 <clockcache_get+0x203>
   53dfa:	mov    %r15,%rax
   53dfd:	mov    %rbp,%r12
   53e00:	mov    %r14,%r15
   53e03:	mov    %r13,0x8(%rsp)
   53e08:	mov    %rax,%r14
   53e0b:	shl    $0x6,%r12
   53e0f:	add    0x48(%rax),%r12
   53e13:	mov    0x8(%rax),%rax
   53e17:	mov    0x18(%rax),%ecx
   53e1a:	test   %ecx,%ecx
   53e1c:	jne    53fbd <clockcache_get+0x45d>
   53e22:	mov    0x18(%r14),%rdi
   53e26:	mov    (%r12),%rsi
   53e2a:	mov    %rbx,%rcx
   53e2d:	mov    %r15,%rdx
   53e30:	mov    (%rdi),%rax
   53e33:	call   *(%rax)
   53e35:	mov    %eax,%r13d
   53e38:	test   %eax,%eax
   53e3a:	jne    53fe4 <clockcache_get+0x484>
   53e40:	mov    0x8(%r14),%rax
   53e44:	mov    0x18(%rax),%edx
   53e47:	test   %edx,%edx
   53e49:	jne    53f60 <clockcache_get+0x400>
   53e4f:	mov    %ebp,%edx
   53e51:	mov    %r13d,%ecx
   53e54:	mov    %rbx,%rsi
   53e57:	mov    %r14,%rdi
   53e5a:	call   53930 <clockcache_finish_load.lto_priv.0>
   53e5f:	mov    %r12,%rdx
   53e62:	test   %r13d,%r13d
   53e65:	je     53ce8 <clockcache_get+0x188>
   53e6b:	mov    0x8(%r14),%rcx
   53e6f:	mov    %rbp,%rax
   53e72:	xor    %edx,%edx
   53e74:	mov    0x8(%rsp),%rdi
   53e79:	divq   0x140(%rcx)
   53e80:	and    $0x3,%edi
   53e83:	shl    $0x6,%edx
   53e86:	lea    (%rdx,%rax,1),%esi
   53e89:	mov    0x130(%rcx),%eax
   53e8f:	mov    0x88(%r14),%rdx
   53e96:	mov    $0xffffffff,%ecx
   53e9b:	imul   %rdi,%rax
   53e9f:	add    %rsi,%rax
   53ea2:	lock xadd %cx,(%rdx,%rax,2)
   53ea8:	test   %cx,%cx
   53eab:	jne    53d0a <clockcache_get+0x1aa>
   53eb1:	push   %rax
   53eb2:	push   %rdi
   53eb3:	push   %rsi
   53eb4:	push   0x20(%rsp)
   53eb8:	xor    %r9d,%r9d
   53ebb:	lea    0x64336(%rip),%r8        # b81f8 <__func__.21+0xd68>
   53ec2:	xor    %eax,%eax
   53ec4:	mov    $0x290,%esi
   53ec9:	lea    0x5d75e(%rip),%rcx        # b162e <_fini+0xb12>
   53ed0:	lea    0x6eaf9(%rip),%rdx        # c29d0 <__func__.28.lto_priv.1>
   53ed7:	lea    0x5d6f9(%rip),%rdi        # b15d7 <_fini+0xabb>
   53ede:	call   ee00 <platform_assert_false@plt>
   53ee3:	nopl   0x0(%rax,%rax,1)
   53ee8:	mov    0x10(%rsp),%rsi
   53eed:	mov    0x1c(%rsp),%ecx
   53ef1:	lea    0x0(,%rsi,8),%rax
   53ef9:	sub    %rsi,%rax
   53efc:	lea    0x220(%rcx,%rax,8),%rax
   53f04:	addq   $0x1,(%r14,%rax,8)
   53f09:	jmp    53ce8 <clockcache_get+0x188>
   53f0e:	mov    0x8(%r15),%rcx
   53f12:	xor    %edx,%edx
   53f14:	mov    0x10(%rsp),%rdi
   53f19:	divq   0x140(%rcx)
   53f20:	and    $0x3,%edi
   53f23:	shl    $0x6,%edx
   53f26:	lea    (%rdx,%rax,1),%esi
   53f29:	mov    0x130(%rcx),%eax
   53f2f:	mov    0x88(%r15),%rdx
   53f36:	mov    $0xffffffff,%ecx
   53f3b:	imul   %rdi,%rax
   53f3f:	add    %rsi,%rax
   53f42:	lock xadd %cx,(%rdx,%rax,2)
   53f48:	test   %cx,%cx
   53f4b:	jne    53d63 <clockcache_get+0x203>
   53f51:	push   %r8
   53f53:	push   %rdi
   53f54:	push   %rsi
   53f55:	push   0x28(%rsp)
   53f59:	jmp    53eb8 <clockcache_get+0x358>
   53f5e:	xchg   %ax,%ax
   53f60:	lea    0x30(%rsp),%rsi
   53f65:	mov    $0x1,%edi
   53f6a:	call   e410 <clock_gettime@plt>
   53f6f:	mov    0x28(%rsp),%rax
   53f74:	mov    0x8(%rsp),%rsi
   53f79:	imul   $0x3b9aca00,0x30(%rsp),%rdx
   53f82:	add    0x38(%rsp),%rdx
   53f87:	sub    %rax,%rdx
   53f8a:	mov    0x1c(%rsp),%ecx
   53f8e:	lea    0x0(,%rsi,8),%rax
   53f96:	sub    %rsi,%rax
   53f99:	lea    (%rcx,%rax,8),%rax
   53f9d:	lea    (%r14,%rax,8),%rax
   53fa1:	addq   $0x1,0x1148(%rax)
   53fa9:	addq   $0x1,0x1220(%rax)
   53fb1:	add    %rdx,0x1190(%rax)
   53fb8:	jmp    53e4f <clockcache_get+0x2ef>
   53fbd:	lea    0x30(%rsp),%rsi
   53fc2:	mov    $0x1,%edi
   53fc7:	call   e410 <clock_gettime@plt>
   53fcc:	imul   $0x3b9aca00,0x30(%rsp),%rax
   53fd5:	add    0x38(%rsp),%rax
   53fda:	mov    %rax,0x28(%rsp)
   53fdf:	jmp    53e22 <clockcache_get+0x2c2>
   53fe4:	mov    %eax,%edi
   53fe6:	call   efe0 <strerror@plt>
   53feb:	mov    0x80fde(%rip),%r15        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53ff2:	mov    0x1c(%rsp),%ecx
   53ff6:	mov    %rbx,%rdx
   53ff9:	mov    %rax,%r8
   53ffc:	lea    0x6489d(%rip),%rsi        # b88a0 <__func__.21+0x1410>
   54003:	xor    %eax,%eax
   54005:	mov    (%r15),%rdi
   54008:	call   fbf0 <fprintf@plt>
   5400d:	mov    (%r15),%rdi
   54010:	call   fe90 <fflush@plt>
   54015:	mov    0x8(%r14),%rax
   54019:	mov    0x18(%rax),%edx
   5401c:	test   %edx,%edx
   5401e:	je     53e4f <clockcache_get+0x2ef>
   54024:	jmp    53f60 <clockcache_get+0x400>
   54029:	mov    0x8(%r14),%rcx
   5402d:	xor    %edx,%edx
   5402f:	mov    0x10(%rsp),%rdi
   54034:	divq   0x140(%rcx)
   5403b:	and    $0x3,%edi
   5403e:	shl    $0x6,%edx
   54041:	lea    (%rdx,%rax,1),%esi
   54044:	mov    0x130(%rcx),%eax
   5404a:	mov    0x88(%r14),%rdx
   54051:	mov    $0xffffffff,%ecx
   54056:	imul   %rdi,%rax
   5405a:	add    %rsi,%rax
   5405d:	lock xadd %cx,(%rdx,%rax,2)
   54063:	test   %cx,%cx
   54066:	jne    53d0a <clockcache_get+0x1aa>
   5406c:	jmp    53f51 <clockcache_get+0x3f1>
   54071:	mov    0x1c(%rsp),%esi
   54075:	mov    %ebp,%r9d
   54078:	lea    0x5d68b(%rip),%r8        # b170a <_fini+0xbee>
   5407f:	lea    0x5d69b(%rip),%rcx        # b1721 <_fini+0xc05>
   54086:	lea    0x6e703(%rip),%rdx        # c2790 <__func__.19>
   5408d:	lea    0x5d543(%rip),%rdi        # b15d7 <_fini+0xabb>
   54094:	push   %rsi
   54095:	mov    $0x8e5,%esi
   5409a:	push   %rax
   5409b:	xor    %eax,%eax
   5409d:	call   ee00 <platform_assert_false@plt>
   540a2:	nopw   0x0(%rax,%rax,1)
   540a8:	sub    $0x8,%rsp
   540ac:	lea    0x6458d(%rip),%r8        # b8640 <__func__.21+0x11b0>
   540b3:	xor    %eax,%eax
   540b5:	push   %rsi
   540b6:	mov    0x2c(%rsp),%r9d
   540bb:	lea    0x645a6(%rip),%rcx        # b8668 <__func__.21+0x11d8>
   540c2:	lea    0x6e7b7(%rip),%rdx        # c2880 <__func__.23.lto_priv.1>
   540c9:	mov    $0x1de,%esi
   540ce:	lea    0x5d502(%rip),%rdi        # b15d7 <_fini+0xabb>
   540d5:	call   ee00 <platform_assert_false@plt>
   540da:	mov    %r15,%r14
   540dd:	jmp    1015d <clockcache_get.cold>

Disassembly of section .fini:
