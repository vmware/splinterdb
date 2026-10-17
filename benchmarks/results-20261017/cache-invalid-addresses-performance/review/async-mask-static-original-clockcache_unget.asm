
.cache/invalid-address-perf-20261017/builds/original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000050fe0 <clockcache_unget>:
   50fe0:	mov    0x48(%rdi),%rdx
   50fe4:	mov    (%rsi),%rax
   50fe7:	mov    %rdi,%rcx
   50fea:	mov    0x8(%rsi),%rdi
   50fee:	sub    %rdx,%rax
   50ff1:	sar    $0x6,%rax
   50ff5:	mov    %eax,%eax
   50ff7:	mov    %rax,%rsi
   50ffa:	shl    $0x6,%rsi
   50ffe:	mov    0x10(%rdx,%rsi,1),%edx
   51002:	and    $0x2,%edx
   51005:	je     51050 <clockcache_unget+0x70>
   51007:	mov    0x8(%rcx),%rsi
   5100b:	xor    %edx,%edx
   5100d:	mov    %rdi,%r9
   51010:	and    $0x3,%r9d
   51014:	divq   0x140(%rsi)
   5101b:	shl    $0x6,%edx
   5101e:	lea    (%rdx,%rax,1),%r8d
   51022:	mov    0x130(%rsi),%eax
   51028:	mov    0x88(%rcx),%rdx
   5102f:	mov    $0xffffffff,%ecx
   51034:	imul   %r9,%rax
   51038:	add    %r8,%rax
   5103b:	lock xadd %cx,(%rdx,%rax,2)
   51041:	test   %cx,%cx
   51044:	je     5105c <clockcache_unget+0x7c>
   51046:	ret
   51047:	nopw   0x0(%rax,%rax,1)
   51050:	mov    0x48(%rcx),%rdx
   51054:	lock orl $0x2,0x10(%rdx,%rsi,1)
   5105a:	jmp    51007 <clockcache_unget+0x27>
   5105c:	sub    $0x10,%rsp
   51060:	lea    0x615c7(%rip),%rcx        # b262e <_fini+0x1512>
   51067:	lea    0x72982(%rip),%rdx        # c39f0 <__func__.28.lto_priv.1>
   5106e:	xor    %eax,%eax
   51070:	push   %r9
   51072:	mov    $0x27c,%esi
   51077:	xor    %r9d,%r9d
   5107a:	push   %r8
   5107c:	lea    0x68105(%rip),%r8        # b9188 <__func__.21+0xcf8>
   51083:	push   %rdi
   51084:	lea    0x6154c(%rip),%rdi        # b25d7 <_fini+0x14bb>
   5108b:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
